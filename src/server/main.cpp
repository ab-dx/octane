#include "kvstore/raft_node.hpp"
#include "kvstore/store.hpp"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <string>

void RunServer() {
  std::string server_address("0.0.0.0:50051");

  kvstore::KVStore db(".");
  kvstore::RaftNode raft_service(db);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&raft_service);

  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "Raft Node locked and loaded. Listening on " << server_address
            << "\n";

  server->Wait();
}

int main() {
  RunServer();
  return 0;
}
