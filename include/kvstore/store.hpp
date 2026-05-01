#pragma once

#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace kvstore {

class KVStore {
public:
  KVStore() = default;
  ~KVStore() = default;

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

private:
  // underlying map for kv store
  std::unordered_map<std::string, std::string> store_;
  // shared mutex for shared reading and exclusive writes
  mutable std::shared_mutex rw_mutex_;
};

} // namespace kvstore
