#include "kvstore/raft_node.hpp"
#include "kvstore/store.hpp"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <string>
#include <vector>

// Usage: ./kv_server <Node_ID> <My_Port> <Peer0_Port> <Peer1_Port> <Peer2_Port>
int main(int argc, char **argv) {
  if (argc < 4) {
    std::cerr << "Usage: " << argv[0]
              << " <ID> <Port> <PeerPort1> <PeerPort2> ...\n";
    return 1;
  }

  int my_id = std::stoi(argv[1]);
  std::string my_port = argv[2];
  std::string server_address("0.0.0.0:" + my_port);

  std::vector<std::string> peer_addresses;
  for (int i = 3; i < argc; ++i) {
    peer_addresses.push_back("0.0.0.0:" + std::string(argv[i]));
  }

  // separate directory for each node so WAL files don't collide
  std::string db_dir = "node_" + std::to_string(my_id) + "_data";
  std::string mkdir_cmd = "mkdir -p " + db_dir;
  system(mkdir_cmd.c_str());

  kvstore::KVStore db(db_dir);
  kvstore::RaftNode raft_service(db, my_id, peer_addresses);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&raft_service);

  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "Raft Node " << my_id << " listening on " << server_address
            << "\n";
  server->Wait();

  return 0;
}
