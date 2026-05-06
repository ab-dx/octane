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

      {
        std::lock_guard<std::mutex> lock(state_mutex_);
        args.set_term(current_term_);
        args.set_leaderid(node_id_);
      }

      peer.stub->AppendEntries(&context, args, &reply);
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

  // acknowledge the leader
  BecomeFollower(request->term());

  last_heartbeat_ = std::chrono::steady_clock::now();

  reply->set_term(current_term_);
  reply->set_success(true);

  return grpc::Status::OK;
}

} // namespace kvstore
