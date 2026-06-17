#pragma once

#include "kvstore/sstable_reader.hpp"
#include "kvstore/wal.hpp"
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace kvstore {

class KVStore {
public:
  explicit KVStore(const std::string &directory);
  ~KVStore();

  // delete copy/move constructors
  // to prevent accidental copying of db
  KVStore(const KVStore &) = delete;
  KVStore &operator=(const KVStore &) = delete;

  // returns the value if it exists, std::nullopt otherwise
  std::optional<std::string> get(const std::string &key) const;

  // inserts or updates a key-value pair
  void set(const std::string &key, const std::string &value);

  // delete a key
  bool remove(const std::string &key);

  void set_flush_callback(std::function<void()> callback) {
    on_flush_callback_ = std::move(callback);
  }

private:
  void check_and_flush();
  void flush_memtable_to_sstable();

  std::vector<std::string> get_all_sstable_filepaths() const;
  void perform_major_compaction();
  void compaction_worker();

  std::string directory_;

  // in-memory tree
  MemTable memtable_;
  // on disk sstables
  std::vector<std::unique_ptr<SSTableReader>> sstables_;

  // tracker for memory usage to trigger flushes
  size_t estimated_memtable_size_;

  mutable std::shared_mutex rw_mutex_;
  std::unique_ptr<WriteAheadLog> wal_;

  // global monotonically increasing sequence number
  std::atomic<uint64_t> sequence_number_;
  uint64_t current_log_id_;

  // for compaction worker thread
  std::atomic<bool> running_;
  std::thread compaction_thread_;

  std::function<void()> on_flush_callback_;
};

} // namespace kvstore
