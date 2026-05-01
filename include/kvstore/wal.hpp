#pragma once

#include <fstream>
#include <mutex>
#include <string>
#include <vector>

namespace kvstore {

// single log entry from write ahead log
struct LogEntry {
  enum class Type { SET, REMOVE };
  Type type;
  std::string key;
  std::string value;
};

class WriteAheadLog {
public:
  explicit WriteAheadLog(const std::string &filepath);
  ~WriteAheadLog();

  // prevent copying
  WriteAheadLog(const WriteAheadLog &) = delete;
  WriteAheadLog &operator=(const WriteAheadLog &) = delete;

  // append operations to the log
  void append_set(const std::string &key, const std::string &value);
  void append_remove(const std::string &key);

  // read the log from disk to rebuild the in-memory state
  std::vector<LogEntry> recover();

private:
  std::string filepath_;
  std::ofstream file_;
  std::mutex log_mutex_; // for thread-safe file appends
};

} // namespace kvstore
