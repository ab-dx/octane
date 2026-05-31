#include "kvstore/store.hpp"
#include <iostream>

namespace kvstore {

// 4MB MemTable threshold size
constexpr size_t MEMTABLE_FLUSH_THRESHOLD = 4 * 1024 * 1024;

KVStore::KVStore(const std::string &directory)
    : directory_(directory), estimated_memtable_size_(0), sequence_number_(0),
      current_log_id_(1) {

  wal_ = std::make_unique<WriteAheadLog>(directory_);

  // stream the WAL directly into the MemTable
  wal_->recover(memtable_);

  // restore the sequence number and calculate rough size
  for (const auto &[k, v] : memtable_) {
    if (v.seq_num > sequence_number_) {
      sequence_number_ = v.seq_num;
    }
    estimated_memtable_size_ +=
        k.size() + v.value.size() + 16; // 16 bytes for overhead
  }

  std::cout << "[KVStore] Recovered MemTable with " << memtable_.size()
            << " entries. Sequence Number at " << sequence_number_ << "\n";
}

std::optional<std::string> KVStore::get(const std::string &key) const {
  std::shared_lock<std::shared_mutex> lock(rw_mutex_);

  auto it = memtable_.find(key);
  if (it != memtable_.end()) {
    // if the key exists but is a tombstone, it is deleted
    if (it->second.is_tombstone) {
      return std::nullopt;
    }
    return it->second.value;
  }

  // TODO: if not in the MemTable, search the SSTables

  return std::nullopt;
}

void KVStore::set(const std::string &key, const std::string &value) {
  uint64_t seq = ++sequence_number_;

  std::unique_lock<std::shared_mutex> lock(rw_mutex_);

  wal_->append(0, key, value, seq);

  memtable_[key] = ValueRecord{value, seq, false};
  estimated_memtable_size_ += key.size() + value.size() + 16;

  check_and_flush();
}

bool KVStore::remove(const std::string &key) {
  uint64_t seq = ++sequence_number_;

  std::unique_lock<std::shared_mutex> lock(rw_mutex_);

  wal_->append(1, key, "", seq);

  // set as tombstone
  memtable_[key] = ValueRecord{"", seq, true};
  estimated_memtable_size_ += key.size() + 16;

  check_and_flush();

  return true;
}

void KVStore::check_and_flush() {
  if (estimated_memtable_size_ >= MEMTABLE_FLUSH_THRESHOLD) {
    flush_memtable_to_sstable();
  }
}

void KVStore::flush_memtable_to_sstable() {
  std::cout << "\n[KVStore] MemTable size (" << estimated_memtable_size_
            << " bytes) reached threshold. Initiating flush to SSTable...\n";

  // TODO: SSTable Serialization.

  // clear the active MemTable
  memtable_.clear();
  estimated_memtable_size_ = 0;

  // rotate the WAL
  current_log_id_++;
  wal_->rotate_log(current_log_id_);

  std::cout << "[KVStore] Flush complete. WAL rotated to log ID "
            << current_log_id_ << "\n";
}

} // namespace kvstore
