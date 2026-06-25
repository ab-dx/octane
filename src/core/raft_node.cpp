#include "kvstore/raft_node.hpp"
#include <iostream>

namespace kvstore {

RaftNode::RaftNode(KVStore &store, int my_id,
                   const std::vector<std::string> &peer_addresses)
    : store_(store), state_(NodeState::FOLLOWER), current_term_(0),
      voted_for_(-1), node_id_(my_id), rng_(std::random_device{}()),
      timeout_dist_(150, 300), running_(true), commit_index_(-1),
      last_applied_(-1) {

  // build connections to peers
  int current_peer_id = 0;
  for (const auto &addr : peer_addresses) {
    if (current_peer_id == node_id_) {
      current_peer_id++;
    }
    peers_.push_back(
        {current_peer_id, raftpb::RaftNode::NewStub(grpc::CreateChannel(
                              addr, grpc::InsecureChannelCredentials()))});
    std::cout << "[Network] Node " << node_id_ << " configured to talk to Peer "
              << current_peer_id << " at " << addr << "\n";
    current_peer_id++;
  }
  // start async event loop thread
  cq_thread_ = std::thread(&RaftNode::AsyncCompleteRpc, this);

  last_heartbeat_ = std::chrono::steady_clock::now();
  background_thread_ = std::thread(&RaftNode::ElectionLoop, this);
  applier_thread_ = std::thread(&RaftNode::ApplierLoop, this);
}

RaftNode::~RaftNode() {
  running_ = false;

  // shutdown applier workers
  applier_cv_.notify_all();
  if (applier_thread_.joinable())
    applier_thread_.join();

  // shutdown completion queue
  cq_.Shutdown();
  if (cq_thread_.joinable())
    cq_thread_.join();

  if (background_thread_.joinable()) {
    background_thread_.join();
  }
}

void RaftNode::BecomeFollower(int new_term) {
  state_ = NodeState::FOLLOWER;
  current_term_ = new_term;
  voted_for_ = -1;
}

void RaftNode::ElectionLoop() {
  auto last_heartbeat_sent = std::chrono::steady_clock::now();
  int timeout_ms = timeout_dist_(rng_); // randomized timeout
  while (running_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    if (!running_)
      break;

    bool trigger_election = false;
    // lock the state and check the clock
    {
      std::lock_guard<std::mutex> lock(state_mutex_);
      auto now = std::chrono::steady_clock::now();
      auto time_since_heard_leader =
          std::chrono::duration_cast<std::chrono::milliseconds>(now -
                                                                last_heartbeat_)
              .count();
      auto time_since_sent_beats =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              now - last_heartbeat_sent)
              .count();

      // if not a leader, start election
      if (state_ != NodeState::LEADER &&
          time_since_heard_leader >= timeout_ms) {
        state_ = NodeState::CANDIDATE;
        current_term_++;
        voted_for_ = node_id_;
        last_heartbeat_ = now;
        trigger_election = true;
        timeout_ms = timeout_dist_(
            rng_); // random timeout for next election, avoid deadlocks
      } else if (state_ == NodeState::LEADER && time_since_sent_beats >= 50) {
        // if this is the leader, send heartbeats
        SendHeartbeats();
        last_heartbeat_sent = now;
      }
    } // state mutex unlocks here
    if (trigger_election) {
      StartElection();
    }
  }
}

void RaftNode::StartElection() {
  int election_term;

  // extract the term inside a lock
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    election_term = current_term_;
    votes_received_ = 1; // vote for itself
  }

  std::cout << "\n[Election] Node " << node_id_
            << " starting election for Term " << election_term << "\n";

  for (const auto &peer : peers_) {
    uint64_t rpc_id;
    {
      std::lock_guard<std::mutex> lock(rpc_mutex_);
      rpc_id = ++next_rpc_id_;
    }

    auto call = std::make_unique<AsyncVoteCall>();
    call->peer_id = peer.id;
    call->req_term = election_term;

    call->context.set_deadline(std::chrono::system_clock::now() +
                               std::chrono::milliseconds(50));
    call->request.set_term(election_term);
    call->request.set_candidateid(node_id_);

    call->response_reader =
        peer.stub->PrepareAsyncRequestVote(&call->context, call->request, &cq_);
    call->response_reader->StartCall();

    // tag with ID
    call->response_reader->Finish(&call->reply, &call->status,
                                  reinterpret_cast<void *>(rpc_id));

    // move into registry
    {
      std::lock_guard<std::mutex> lock(rpc_mutex_);
      in_flight_votes_[rpc_id] = std::move(call);
    }
  }
}

void RaftNode::SendHeartbeats() {
  if (state_ != NodeState::LEADER)
    return;

  for (const auto &peer : peers_) {
    uint64_t rpc_id;
    {
      std::lock_guard<std::mutex> lock(rpc_mutex_);
      rpc_id = ++next_rpc_id_;
    }

    auto call = std::make_unique<AsyncClientCall>();
    call->peer_id = peer.id;
    call->req_term = current_term_;

    call->context.set_deadline(std::chrono::system_clock::now() +
                               std::chrono::milliseconds(50));
    call->request.set_term(current_term_);
    call->request.set_leaderid(node_id_);
    call->request.set_leadercommit(commit_index_);

    int peer_next_idx = next_index_[peer.id];
    int prev_log_index = peer_next_idx - 1;
    int prev_log_term = GetTerm(prev_log_index);

    // limit to 1000 entries per RPC
    size_t end_idx =
        std::min(log_.size(), static_cast<size_t>(peer_next_idx + 1000));

    call->request.set_prevlogindex(prev_log_index);
    call->request.set_prevlogterm(prev_log_term);

    int local_start_idx = peer_next_idx - last_included_index_ - 1;
    local_start_idx = std::max(0, local_start_idx);
    for (size_t i = peer_next_idx; i < end_idx; ++i) {
      *call->request.add_entries() = log_[i];
    }

    call->response_reader = peer.stub->PrepareAsyncAppendEntries(
        &call->context, call->request, &cq_);
    call->response_reader->StartCall();

    // tag with ID
    call->response_reader->Finish(&call->reply, &call->status,
                                  reinterpret_cast<void *>(rpc_id));

    // store in map
    {
      std::lock_guard<std::mutex> lock(rpc_mutex_);
      in_flight_rpcs_[rpc_id] = std::move(call);
    }
  }
}

grpc::Status RaftNode::RequestVote(grpc::ServerContext *context,
                                   const raftpb::RequestVoteArgs *request,
                                   raftpb::RequestVoteReply *reply) {
  std::lock_guard<std::mutex> lock(state_mutex_);

  // reject older terms
  if (request->term() < current_term_) {
    reply->set_term(current_term_);
    reply->set_votegranted(false);
    return grpc::Status::OK;
  }

  // if the request has a newer term, instantly step down
  if (request->term() > current_term_) {
    BecomeFollower(request->term());
  }

  // if not voted yet or already voted for this candidate, grant vote
  if (voted_for_ == -1 || voted_for_ == request->candidateid()) {
    voted_for_ = request->candidateid();
    last_heartbeat_ = std::chrono::steady_clock::now();

    reply->set_term(current_term_);
    reply->set_votegranted(true);
  } else {
    reply->set_term(current_term_);
    reply->set_votegranted(false);
  }

  return grpc::Status::OK;
}

grpc::Status RaftNode::AppendEntries(grpc::ServerContext *context,
                                     const raftpb::AppendEntriesArgs *request,
                                     raftpb::AppendEntriesReply *reply) {
  std::lock_guard<std::mutex> lock(state_mutex_);

  // reject older terms
  if (request->term() < current_term_) {
    reply->set_term(current_term_);
    reply->set_success(false);
    return grpc::Status::OK;
  }

  // acknowledge leader
  BecomeFollower(request->term());
  last_heartbeat_ = std::chrono::steady_clock::now();
  reply->set_term(current_term_);

  // consistency check
  int prev_idx = request->prevlogindex();
  if (prev_idx >= 0) {
    if (prev_idx >= log_.size() ||
        log_[prev_idx].term() != request->prevlogterm()) {
      reply->set_success(false);
      return grpc::Status::OK;
    }
  }

  // conflict resolution and append
  int log_insert_index = prev_idx + 1;
  for (int i = 0; i < request->entries_size(); ++i) {
    const auto &new_entry = request->entries(i);

    // existing entry conflicts with a new one
    if (log_insert_index < log_.size() &&
        log_[log_insert_index].term() != new_entry.term()) {
      log_.erase(log_.begin() + log_insert_index,
                 log_.end()); // delete everything starting from here
    }

    // append new entries not already in the log
    if (log_insert_index >= log_.size()) {
      log_.push_back(new_entry);
      std::cout << "[Replication] Node " << node_id_
                << " replicated log at index " << log_insert_index << "\n";
    }
    log_insert_index++;
  }

  // update commit index
  if (request->leadercommit() > commit_index_) {
    commit_index_ =
        std::min((int)request->leadercommit(), (int)log_.size() - 1);
    ApplyCommittedEntries(); // apply to store
  }

  reply->set_success(true);
  return grpc::Status::OK;
}

void RaftNode::TruncateLog() {
  std::lock_guard<std::mutex> lock(state_mutex_);

  if (last_applied_ <= last_included_index_)
    return;
  int local_index = last_applied_ - last_included_index_ - 1;

  if (local_index < 0 || local_index >= log_.size())
    return;

  last_included_term_ = log_[local_index].term();
  last_included_index_ = last_applied_;

  // erase from log
  log_.erase(log_.begin(), log_.begin() + local_index + 1);

  std::cout << "[Garbage Collection] Raft log truncated up to absolute index "
            << last_included_index_ << ". RAM freed!\n";
}

int RaftNode::GetTerm(int absolute_index) {
  if (absolute_index == last_included_index_) {
    return last_included_term_;
  }
  int local_index = absolute_index - last_included_index_ - 1;
  if (local_index >= 0 && local_index < log_.size()) {
    return log_[local_index].term();
  }
  return 0;
}

grpc::Status RaftNode::SubmitCommand(grpc::ServerContext *context,
                                     const raftpb::ClientRequest *request,
                                     raftpb::ClientReply *reply) {
  std::lock_guard<std::mutex> lock(state_mutex_);

  if (state_ != NodeState::LEADER) {
    reply->set_success(false);
    reply->set_leaderhint("Unknown");
    return grpc::Status::OK;
  }

  std::string cmd = request->command();

  // intercept get requests
  if (cmd.rfind("GET ", 0) == 0) {
    std::string key = cmd.substr(4);
    auto val = store_.get(key);

    reply->set_success(true);
    if (val) {
      reply->set_value(*val);
    } else {
      reply->set_value("NULL");
    }

    std::cout << "[Client] GET key '" << key << "'\n";
    return grpc::Status::OK;
  }

  // append the command to local raft log
  raftpb::LogEntry new_entry;
  new_entry.set_term(current_term_);
  new_entry.set_command(request->command());

  log_.push_back(new_entry);

  int entry_index = log_.size() - 1; // 0 indexed log

  std::cout << "\n[Client] Received Command: '" << request->command()
            << "' | Appended to Raft Log at index " << entry_index << "\n";

  // SendHeartbeats();

  reply->set_success(true);

  return grpc::Status::OK;
}

void RaftNode::ApplyCommittedEntries() {
  bool pushed = false;
  {
    std::lock_guard<std::mutex> lock(applier_mutex_);
    while (last_applied_ < commit_index_) {
      last_applied_++;
      const auto &entry = log_[last_applied_];
      std::string cmd = entry.command();

      if (cmd.rfind("SET ", 0) == 0) {
        size_t space1 = cmd.find(' ');
        size_t space2 = cmd.find(' ', space1 + 1);
        if (space1 != std::string::npos && space2 != std::string::npos) {
          applier_queue_.push_back({0,
                                    cmd.substr(space1 + 1, space2 - space1 - 1),
                                    cmd.substr(space2 + 1)});
          pushed = true;
        }
      } else if (cmd.rfind("DEL", 0) == 0) {
        applier_queue_.push_back({1, cmd.substr(4), ""});
        pushed = true;
      }
    }
  }
  if (pushed)
    applier_cv_.notify_one();
}

void RaftNode::ApplierLoop() {
  while (running_) {
    std::vector<ApplierTask> batch;

    {
      std::unique_lock<std::mutex> lock(applier_mutex_);
      applier_cv_.wait(
          lock, [this]() { return !applier_queue_.empty() || !running_; });
      if (!running_ && applier_queue_.empty())
        break;

      std::swap(batch, applier_queue_);
    }

    if (batch.empty())
      continue;

    // format for KVStore
    std::vector<std::tuple<uint8_t, std::string, std::string>> ops;
    ops.reserve(batch.size());
    for (const auto &task : batch) {
      ops.push_back({task.op_type, task.key, task.val});
    }

    // write to disk
    store_.apply_batch(ops);
  }
}

void RaftNode::HandleAppendEntriesReply(AsyncClientCall *call) {
  std::lock_guard<std::mutex> lock(state_mutex_);

  // ignore if not leader
  if (state_ != NodeState::LEADER)
    return;

  if (call->req_term != current_term_)
    return;

  if (call->reply.term() > current_term_) {
    BecomeFollower(call->reply.term());
    return;
  }

  if (call->reply.success()) {
    // calculate how many entries were successfully replicated
    match_index_[call->peer_id] =
        call->request.prevlogindex() + call->request.entries_size();
    next_index_[call->peer_id] = match_index_[call->peer_id] + 1;

    // check if any new entries can be committed
    for (int n = log_.size() - 1; n > commit_index_; --n) {
      if (log_[n].term() == current_term_) {
        int match_count = 1;
        for (const auto &p : peers_) {
          if (match_index_[p.id] >= n)
            match_count++;
        }
        int majority = (peers_.size() + 1) / 2 + 1;
        if (match_count >= majority) {
          commit_index_ = n;
          ApplyCommittedEntries();
          break;
        }
      }
    }
  } else {
    // decrement next_index and retry next heartbeat
    next_index_[call->peer_id] = std::max(0, next_index_[call->peer_id] - 1);
  }
}

void RaftNode::HandleRequestVoteReply(AsyncVoteCall *call) {
  std::lock_guard<std::mutex> lock(state_mutex_);

  // if we are not the leader, or reply from old term, reject
  if (state_ != NodeState::CANDIDATE || call->req_term != current_term_) {
    return;
  }

  // if reply from a higher term, we are outdated
  if (call->reply.term() > current_term_) {
    BecomeFollower(call->reply.term());
    return;
  }

  // tally the vote
  if (call->reply.votegranted()) {
    votes_received_++;
    std::cout << "[Election] Got vote from Peer " << call->peer_id
              << ". Total: " << votes_received_ << "\n";

    int majority = (peers_.size() + 1) / 2 + 1;

    // check for majority
    if (votes_received_ >= majority) {
      state_ = NodeState::LEADER;

      for (const auto &p : peers_) {
        next_index_[p.id] = log_.size();
        match_index_[p.id] = -1;
      }

      std::cout << "NODE " << node_id_ << " IS THE NEW LEADER FOR TERM "
                << current_term_ << "\n";

      SendHeartbeats();
    }
  }
}

void RaftNode::AsyncCompleteRpc() {
  void *got_tag;
  bool ok = false;

  while (cq_.Next(&got_tag, &ok)) {
    uint64_t rpc_id = reinterpret_cast<uint64_t>(got_tag);

    std::unique_ptr<AsyncClientCall> append_call;
    std::unique_ptr<AsyncVoteCall> vote_call;

    // search both maps
    {
      std::lock_guard<std::mutex> lock(rpc_mutex_);

      auto it_append = in_flight_rpcs_.find(rpc_id);
      if (it_append != in_flight_rpcs_.end()) {
        append_call = std::move(it_append->second);
        in_flight_rpcs_.erase(it_append);
      } else {
        auto it_vote = in_flight_votes_.find(rpc_id);
        if (it_vote != in_flight_votes_.end()) {
          vote_call = std::move(it_vote->second);
          in_flight_votes_.erase(it_vote);
        }
      }
    }

    // process if network didn't fail
    if (ok) {
      if (append_call && append_call->status.ok()) {
        HandleAppendEntriesReply(append_call.get());
      } else if (vote_call && vote_call->status.ok()) {
        HandleRequestVoteReply(vote_call.get());
      }
    }
  }
}

} // namespace kvstore
