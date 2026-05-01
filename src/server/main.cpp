#include "kvstore/store.hpp"
#include <iostream>

int main() {
  kvstore::KVStore db;

  db.set("user:101", "Abhinav");

  if (auto val = db.get("user:101")) {
    std::cout << "Found: " << *val << "\n";
  }

  return 0;
}
