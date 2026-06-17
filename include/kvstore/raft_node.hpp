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

struct AsyncClientCall {
  raftpb::AppendEntriesArgs request;
  raftpb::AppendEntriesReply reply;
  grpc::ClientContext context;
  grpc::Status status;
  std::unique_ptr<grpc::ClientAsyncResponseReader<raftpb::AppendEntriesReply>>
      response_reader;
  int peer_id;
  int req_term;
};

struct AsyncVoteCall {
  raftpb::RequestVoteArgs request;
  raftpb::RequestVoteReply reply;
  grpc::ClientContext context;
  grpc::Status status;
  std::unique_ptr<grpc::ClientAsyncResponseReader<raftpb::RequestVoteReply>>
      response_reader;
  int peer_id;
  int req_term;
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

  grpc::Status SubmitCommand(grpc::ServerContext *context,
                             const raftpb::ClientRequest *request,
                             raftpb::ClientReply *reply) override;

private:
  KVStore &store_;

  std::mutex state_mutex_; // protects all state below
  NodeState state_;
  int current_term_;
  int voted_for_;
  int node_id_;
  int votes_received_ = 0;

  std::vector<Peer> peers_; // other peers in the network

  std::chrono::time_point<std::chrono::steady_clock> last_heartbeat_;
  std::mt19937 rng_; // random number generator, randomized timeouts to prevent
                     // election ties
  std::uniform_int_distribution<int> timeout_dist_;

  std::atomic<bool> running_;
  std::thread background_thread_;

  // raft log
  std::vector<raftpb::LogEntry> log_;
  int commit_index_ = 0; // index of highest log entry known to be committed
  int last_applied_ = 0;

  // track the next log index to send to each peer
  std::unordered_map<int, int> next_index_;
  // track the highest log index known to be replicated on each peer
  std::unordered_map<int, int> match_index_;

  grpc::CompletionQueue cq_; // job queue for async tasks
  std::thread cq_thread_;

  // rpc registry
  uint64_t next_rpc_id_ = 0;
  std::mutex rpc_mutex_; // protects ID counter and two maps below

  std::unordered_map<uint64_t, std::unique_ptr<AsyncClientCall>>
      in_flight_rpcs_;
  std::unordered_map<uint64_t, std::unique_ptr<AsyncVoteCall>> in_flight_votes_;

  void AsyncCompleteRpc();
  void ElectionLoop();
  void BecomeFollower(int new_term);
  void StartElection();
  void SendHeartbeats();
  void ApplyCommittedEntries();
  void HandleAppendEntriesReply(AsyncClientCall *call);
  void HandleRequestVoteReply(AsyncVoteCall *call);
};

} // namespace kvstore
