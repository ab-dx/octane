#include "kvstore/store.hpp"

namespace kvstore {

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

  store_[key] = value;

  // TODO: append to write-ahead log (WAL) here
}

bool KVStore::remove(const std::string &key) {
  // unique lock blocks all other readers and writers
  std::unique_lock<std::shared_mutex> lock(rw_mutex_);

  bool erased = store_.erase(key) > 0;

  if (erased) {
    // TODO: append tombstone/delete record to WAL here
  }

  return erased;
}

} // namespace kvstore
