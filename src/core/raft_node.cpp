#include "kvstore/raft_node.hpp"
#include <iostream>

namespace kvstore {

RaftNode::RaftNode(KVStore &store, int my_id,
                   const std::vector<std::string> &peer_addresses)
    : store_(store), state_(NodeState::FOLLOWER), current_term_(0),
      voted_for_(-1), node_id_(my_id), rng_(std::random_device{}()),
      timeout_dist_(150, 300), running_(true) {

  // build connections to peers
  int peer_id = 0;
  for (const auto &addr : peer_addresses) {
    if (peer_id != node_id_) { // don't connect to self
      peers_.push_back(
          {peer_id, raftpb::RaftNode::NewStub(grpc::CreateChannel(
                        addr, grpc::InsecureChannelCredentials()))});
      std::cout << "[Network] Node " << node_id_
                << " configured to talk to Peer " << peer_id << " at " << addr
                << "\n";
    }
    peer_id++;
  }

  last_heartbeat_ = std::chrono::steady_clock::now();
  background_thread_ = std::thread(&RaftNode::ElectionLoop, this);
}

RaftNode::~RaftNode() {
  running_ = false;

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
  std::cout << "\n[Election] Node " << node_id_
            << " starting election for Term " << current_term_ << "\n";

  std::shared_ptr<std::atomic<int>> votes_received =
      std::make_shared<std::atomic<int>>(1);
  int majority = (peers_.size() + 1) / 2 + 1;

  for (const auto &peer : peers_) {
    std::thread([this, &peer, votes_received, majority]() {
      raftpb::RequestVoteArgs args;
      raftpb::RequestVoteReply reply;
      grpc::ClientContext context;

      // wait for 50ms to reply
      context.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::milliseconds(50));

      {
        std::lock_guard<std::mutex> lock(state_mutex_);
        args.set_term(current_term_);
        args.set_candidateid(node_id_);
      }

      grpc::Status status = peer.stub->RequestVote(&context, args, &reply);

      if (status.ok() && reply.votegranted()) {
        (*votes_received)++;
        std::cout << "[Election] Got vote from Peer " << peer.id
                  << ". Total: " << *votes_received << "\n";

        // when majority votes received, become leader
        if (*votes_received >= majority) {
          std::lock_guard<std::mutex> lock(state_mutex_);
          if (state_ == NodeState::CANDIDATE) {
            state_ = NodeState::LEADER;

            for (const auto &p : peers_) {
              // next index to send is right after current log end
              next_index_[p.id] = log_.size();
              match_index_[p.id] = -1; // nothing replicated yet
            }

            std::cout << "NODE " << node_id_ << " IS THE NEW LEADER FOR TERM "
                      << current_term_ << "\n";
            // start sending heartbeats
            SendHeartbeats();
          }
        }
      }
    }).detach();
  }
}

void RaftNode::SendHeartbeats() {
  for (const auto &peer : peers_) {
    std::thread([this, &peer]() {
      raftpb::AppendEntriesArgs args;
      raftpb::AppendEntriesReply reply;
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::milliseconds(50));

      int peer_next_idx;

      {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (state_ != NodeState::LEADER)
          return; // exit if not leader

        args.set_term(current_term_);
        args.set_leaderid(node_id_);
        args.set_leadercommit(commit_index_);

        peer_next_idx = next_index_[peer.id];
        int prev_log_index = peer_next_idx - 1;

        // calculate previous term
        int prev_log_term =
            (prev_log_index >= 0 && prev_log_index < log_.size())
                ? log_[prev_log_index].term()
                : 0;

        args.set_prevlogindex(prev_log_index);
        args.set_prevlogterm(prev_log_term);

        // add any missing entries to the RPC
        for (size_t i = peer_next_idx; i < log_.size(); ++i) {
          *args.add_entries() = log_[i];
        }
      }

      grpc::Status status = peer.stub->AppendEntries(&context, args, &reply);

      if (status.ok()) {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (state_ != NodeState::LEADER || current_term_ != args.term())
          return;

        if (reply.term() > current_term_) {
          BecomeFollower(reply.term());
          return;
        }

        if (reply.success()) {
          match_index_[peer.id] = args.prevlogindex() + args.entries_size();
          next_index_[peer.id] = match_index_[peer.id] + 1;

          // commit a new entry if a majority replicated
          for (int n = log_.size() - 1; n > commit_index_; --n) {
            if (log_[n].term() == current_term_) {
              int match_count = 1;
              for (const auto &p : peers_) {
                if (match_index_[p.id] >= n)
                  match_count++;
              }

              int majority = (peers_.size() + 1) / 2 + 1;
              if (match_count >= majority) {
                commit_index_ = n;       // move the commit line forward
                ApplyCommittedEntries(); // apply to store
                break;
              }
            }
          }
        } else {
          // decrement next_index and retry with older logs next heartbeat
          next_index_[peer.id] = std::max(0, next_index_[peer.id] - 1);
        }
      }
    }).detach();
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

  SendHeartbeats();

  reply->set_success(true);

  return grpc::Status::OK;
}

void RaftNode::ApplyCommittedEntries() {
  // if commit index has moved forward, apply all unapplied entries
  while (last_applied_ < commit_index_) {
    last_applied_++;
    const auto &entry = log_[last_applied_];

    std::string cmd = entry.command();
    if (cmd.rfind("SET ", 0) == 0) {
      size_t space1 = cmd.find(' ');
      size_t space2 = cmd.find(' ', space1 + 1);
      if (space1 != std::string::npos && space2 != std::string::npos) {
        std::string key = cmd.substr(space1 + 1, space2 - space1 - 1);
        std::string val = cmd.substr(space2 + 1);

        store_.set(key, val);
        std::cout << "\n[Store] Committed to Disk: " << key << " = " << val
                  << "\n";
      }
    } else if (cmd.rfind("DEL", 0) == 0) {
      std::string key = cmd.substr(4); // skip "DEL "
      store_.remove(key);
      std::cout << "[Store] Deleted from Disk: " << key << "\n";
    }
  }
}

} // namespace kvstore
