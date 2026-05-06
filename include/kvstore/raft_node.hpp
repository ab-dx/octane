#pragma once

#include "kvstore/store.hpp"
#include "raft.grpc.pb.h"
#include <atomic>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace kvstore {

enum class NodeState { FOLLOWER, CANDIDATE, LEADER };

struct Peer {
  int id;
  std::unique_ptr<raftpb::RaftNode::Stub> stub;
};

class RaftNode final : public raftpb::RaftNode::Service {
public:
  RaftNode(KVStore &store, int my_id,
           const std::vector<std::string> &peer_addresses);
  ~RaftNode();

  grpc::Status RequestVote(grpc::ServerContext *context,
                           const raftpb::RequestVoteArgs *request,
                           raftpb::RequestVoteReply *reply) override;

  grpc::Status AppendEntries(grpc::ServerContext *context,
                             const raftpb::AppendEntriesArgs *request,
                             raftpb::AppendEntriesReply *reply) override;

private:
  KVStore &store_;

  std::mutex state_mutex_; // protects all state below
  NodeState state_;
  int current_term_;
  int voted_for_;
  int node_id_;

  std::vector<Peer> peers_; // other peers in the network

  std::chrono::time_point<std::chrono::steady_clock> last_heartbeat_;
  std::mt19937 rng_; // random number generator, randomized timeouts to prevent
                     // election ties
  std::uniform_int_distribution<int> timeout_dist_;

  std::atomic<bool> running_;
  std::thread background_thread_;

  void ElectionLoop();
  void BecomeFollower(int new_term);
  void StartElection();
  void SendHeartbeats();
};

} // namespace kvstore
