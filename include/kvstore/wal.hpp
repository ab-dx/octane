#pragma once

#include <cstdint>
#include <fstream>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace kvstore {

// value in the LSM Memtable
struct ValueRecord {
  std::string value;
  uint64_t seq_num;
  bool is_tombstone; // mark deletions
};

struct BatchEntry {
  uint8_t op_type;
  std::string key;
  std::string value;
  uint64_t seq_num;
};

using MemTable = std::map<std::string, ValueRecord>;

class WriteAheadLog {
public:
  explicit WriteAheadLog(const std::string &directory);
  ~WriteAheadLog();

  // prevent copying
  WriteAheadLog(const WriteAheadLog &) = delete;
  WriteAheadLog &operator=(const WriteAheadLog &) = delete;

  // append operations to the log
  void append(uint8_t op_type, const std::string &key, const std::string &value,
              uint64_t seq_num);
  // append entries in batches
  void append_batch(const std::vector<BatchEntry> &batch);

  // log rotations
  void rotate_log(uint64_t new_log_id);

  // recovery from logs
  void recover(MemTable &memtable);

private:
  std::string directory_;
  uint64_t current_log_id_;
  std::ofstream file_;
  std::mutex log_mutex_; // for thread-safe log operations

  std::string get_log_filepath(uint64_t log_id) const;
  void open_log_file(uint64_t log_id);
};

} // namespace kvstore
