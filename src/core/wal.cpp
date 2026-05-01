#include "kvstore/wal.hpp"
#include <iostream>
#include <sstream>

namespace kvstore {

WriteAheadLog::WriteAheadLog(const std::string &filepath)
    : filepath_(filepath) {
  file_.open(filepath_, std::ios::app);
  if (!file_.is_open()) {
    throw std::runtime_error("Failed to open WAL file: " + filepath_);
  }
}

WriteAheadLog::~WriteAheadLog() {
  if (file_.is_open()) {
    file_.close();
  }
}

void WriteAheadLog::append_set(const std::string &key,
                               const std::string &value) {
  std::lock_guard<std::mutex> lock(log_mutex_); // thread safe write
  file_ << "S " << key << " " << value << "\n";
  file_.flush();
}

void WriteAheadLog::append_remove(const std::string &key) {
  std::lock_guard<std::mutex> lock(log_mutex_); // thread safe write
  file_ << "D " << key << "\n";
  file_.flush();
}

std::vector<LogEntry> WriteAheadLog::recover() {
  std::vector<LogEntry> entries;
  std::ifstream infile(filepath_);

  if (!infile.is_open())
    return entries;

  std::string line;
  while (std::getline(infile, line)) {
    if (line.empty())
      continue;

    char op = line[0];
    std::istringstream iss(line.substr(2)); // skip "S " or "D "
    std::string key, value;

    if (op == 'S') {
      iss >> key;
      // extract the rest of the line
      std::getline(iss >> std::ws, value);
      entries.push_back({LogEntry::Type::SET, key, value});
    } else if (op == 'D') {
      iss >> key;
      entries.push_back({LogEntry::Type::REMOVE, key, ""});
    }
  }
  return entries;
}

} // namespace kvstore
