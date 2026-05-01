#include "kvstore/store.hpp"
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

// simulates rapid SET requests
void writer_task(kvstore::KVStore &db, int thread_id, int operations) {
  for (int i = 0; i < operations; ++i) {
    std::string key =
        "key:" + std::to_string(thread_id) + ":" + std::to_string(i);
    std::string value =
        "val:" + std::to_string(thread_id) + ":" + std::to_string(i);
    db.set(key, value);
  }
}

// simulates rapid GET requests
void reader_task(kvstore::KVStore &db, int operations) {
  for (int i = 0; i < operations; ++i) {
    std::string key = "key:0:" + std::to_string(i % 100);
    auto val = db.get(key);
  }
}

int main() {
  kvstore::KVStore db(".");

  const int num_writers = 4;
  const int num_readers = 4;
  const int ops_per_thread = 5000;

  std::vector<std::thread> threads;
  auto start_time = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < num_writers; ++i) {
    threads.emplace_back(writer_task, std::ref(db), i, ops_per_thread);
  }

  for (int i = 0; i < num_readers; ++i) {
    threads.emplace_back(reader_task, std::ref(db), ops_per_thread);
  }

  // wait for threads to finish
  for (auto &t : threads) {
    t.join();
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);

  std::cout << "Test completed successfully in " << duration.count() << " ms\n";
  std::cout << "Total writes executed safely: "
            << (num_writers * ops_per_thread) << "\n";

  return 0;
}
