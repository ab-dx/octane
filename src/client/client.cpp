#include "raft.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <string>

// Usage: ./kv_client <Target_Port> "COMMAND"
int main(int argc, char **argv) {
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <Port> \"SET key value\"\n";
    return 1;
  }

  std::string target_str = "0.0.0.0:" + std::string(argv[1]);
  std::string command = argv[2];

  // connect to the node
  auto channel =
      grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials());
  auto stub = raftpb::RaftNode::NewStub(channel);

  raftpb::ClientRequest request;
  request.set_command(command);
  raftpb::ClientReply reply;
  grpc::ClientContext context;

  std::cout << "Sending command to " << target_str << "...\n";
  grpc::Status status = stub->SubmitCommand(&context, request, &reply);

  if (status.ok()) {
    if (reply.success()) {
      std::cout << "SUCCESS: Command accepted by leader\n";
      if (!reply.value().empty()) {
        std::cout << "VALUE: " << reply.value() << "\n";
      }
    } else {
      std::cout << "REJECTED: Node is a follower\n";
    }
  } else {
    std::cout << "RPC Failed\n";
  }

  return 0;
}
