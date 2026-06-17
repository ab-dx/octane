#pragma once

#include "kvstore/sstable.hpp"
#include <fstream>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace kvstore {

struct MergedRecord {
  std::string key;
  std::string value;
  uint64_t seq_num;
  bool is_tombstone;
  size_t file_index; // tracks which file this came from before merge
};

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

class SSTableIterator {
public:
  explicit SSTableIterator(const std::string &filepath, size_t file_idx);

  bool is_valid() const;
  void next();
  MergedRecord current() const;

private:
  std::ifstream file_;
  uint64_t index_offset_;
  MergedRecord current_record_;
  bool valid_;
  size_t file_idx_;
};

} // namespace kvstore
