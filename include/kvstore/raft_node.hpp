#pragma once

#include "kvstore/store.hpp"
#include "raft.grpc.pb.h"
#include <grpcpp/grpcpp.h>

namespace kvstore {

class RaftNode final : public raftpb::RaftNode::Service {
public:
  explicit RaftNode(KVStore &store);

  // override gRPC service methods
  grpc::Status RequestVote(grpc::ServerContext *context,
                           const raftpb::RequestVoteArgs *request,
                           raftpb::RequestVoteReply *reply) override;

  grpc::Status AppendEntries(grpc::ServerContext *context,
                             const raftpb::AppendEntriesArgs *request,
                             raftpb::AppendEntriesReply *reply) override;

private:
  KVStore &store_;
};

} // namespace kvstore
