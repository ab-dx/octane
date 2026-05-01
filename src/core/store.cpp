#include "kvstore/store.hpp"
#include <iostream>

namespace kvstore {

KVStore::KVStore(const std::string &directory) {
  std::string wal_path = directory + "/kvstore.wal";
  wal_ = std::make_unique<WriteAheadLog>(wal_path);

  // recover existing data
  auto recovered_entries = wal_->recover();
  int count = 0;

  // populate store_ from log entries
  for (const auto &entry : recovered_entries) {
    if (entry.type == LogEntry::Type::SET) {
      store_[entry.key] = entry.value;
    } else if (entry.type == LogEntry::Type::REMOVE) {
      store_.erase(entry.key);
    }
    count++;
  }

  std::cout << "[KVStore] Recovered " << count << " entries from WAL\n";
}

std::optional<std::string> KVStore::get(const std::string &key) const {
  // shared lock allows multiple concurrent readers
  std::shared_lock<std::shared_mutex> lock(rw_mutex_);

  auto it = store_.find(key);
  if (it != store_.end()) {
    return it->second;
  }
  return std::nullopt;
}

void KVStore::set(const std::string &key, const std::string &value) {
  // unique lock blocks all other readers and writers
  std::unique_lock<std::shared_mutex> lock(rw_mutex_);

  // append to disk first
  wal_->append_set(key, value);
  // then to store
  store_[key] = value;
}

bool KVStore::remove(const std::string &key) {
  // unique lock blocks all other readers and writers
  std::unique_lock<std::shared_mutex> lock(rw_mutex_);

  bool erased = store_.erase(key) > 0;

  if (erased) {
    wal_->append_remove(key);
  }

  return erased;
}

} // namespace kvstore
