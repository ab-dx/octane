#pragma once

#include "kvstore/sstable.hpp"
#include <fstream>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace kvstore {

class SSTableReader {
public:
  explicit SSTableReader(const std::string &filepath);
  ~SSTableReader();

  SSTableReader(const SSTableReader &) = delete;
  SSTableReader &operator=(const SSTableReader &) = delete;

  std::optional<std::string> get(const std::string &key);

private:
  void load_index();

  std::string filepath_;
  std::ifstream file_;

  // TODO: POSIX pread() instead of mutexes + normal reads
  std::mutex read_mutex_;

  std::vector<IndexEntry> index_;
  uint64_t index_offset_;
};

} // namespace kvstore
