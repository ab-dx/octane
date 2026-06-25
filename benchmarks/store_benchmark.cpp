#include "kvstore/sstable.hpp"
#include "kvstore/store.hpp"
#include "kvstore/wal.hpp"
#include <benchmark/benchmark.h>
#include <filesystem>
#include <memory>

class SharedKVStoreFixture : public benchmark::Fixture {
public:
  std::unique_ptr<kvstore::KVStore> store;
  std::string test_dir = "bench_data_shared";

  void SetUp(const ::benchmark::State &state) {
    if (state.thread_index() == 0) {
      std::filesystem::remove_all(test_dir);
      std::filesystem::create_directories(test_dir);

      store = std::make_unique<kvstore::KVStore>(test_dir);
      store->set("shared_key", "shared_value");
    }
  }

  void TearDown(const ::benchmark::State &state) {
    if (state.thread_index() == 0) {
      store.reset();

      std::filesystem::remove_all(test_dir);
    }
  }
};

class IsolatedKVStoreFixture : public benchmark::Fixture {
public:
  std::string test_dir;

  void SetUp(const ::benchmark::State &state) {
    test_dir = "bench_data_iso_" + std::to_string(state.thread_index());
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
  }

  void TearDown(const ::benchmark::State &state) {
    std::filesystem::remove_all(test_dir);
  }
};

BENCHMARK_F(IsolatedKVStoreFixture, SequentialWrites)(benchmark::State &state) {
  kvstore::KVStore store(test_dir);
  int i = 0;
  for (auto _ : state) {
    store.set("key_" + std::to_string(i), "value_payload_" + std::to_string(i));
    i++;
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_F(SharedKVStoreFixture, ConcurrentReads)(benchmark::State &state) {
  for (auto _ : state) {
    auto val = store->get("shared_key");
    benchmark::DoNotOptimize(val);
  }
}
BENCHMARK_REGISTER_F(SharedKVStoreFixture, ConcurrentReads)
    ->Threads(1)
    ->Threads(4)
    ->Threads(8);

BENCHMARK_MAIN();
