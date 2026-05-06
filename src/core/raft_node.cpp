#include "kvstore/raft_node.hpp"
#include <iostream>

namespace kvstore {

RaftNode::RaftNode(KVStore &store) : store_(store) {}

grpc::Status RaftNode::RequestVote(grpc::ServerContext *context,
                                   const raftpb::RequestVoteArgs *request,
                                   raftpb::RequestVoteReply *reply) {
  std::cout << "[Network] Received RequestVote from Candidate: "
            << request->candidateid() << "\n";

  // TODO: voting logic
  reply->set_term(request->term());
  reply->set_votegranted(true);

  return grpc::Status::OK;
}

grpc::Status RaftNode::AppendEntries(grpc::ServerContext *context,
                                     const raftpb::AppendEntriesArgs *request,
                                     raftpb::AppendEntriesReply *reply) {
  std::cout << "[Network] Received AppendEntries from Leader: "
            << request->leaderid() << "\n";

  // TODO: voting logic
  reply->set_term(request->term());
  reply->set_success(true);

  return grpc::Status::OK;
}

} // namespace kvstore
