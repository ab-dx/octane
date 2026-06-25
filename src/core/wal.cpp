#include "kvstore/wal.hpp"
#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <vector>

namespace kvstore {

#pragma pack(push, 1)
struct WALRecordHeader {
  uint32_t crc32;  // error detection
  uint8_t op_type; // 0 for SET, 1 for REMOVE
  uint64_t seq_num;
  uint32_t key_len;
  uint32_t val_len;
};
#pragma pack(pop)

uint32_t calculate_crc32(const uint8_t *data, size_t length,
                         uint32_t previous_crc32 = 0) {
  uint32_t crc = ~previous_crc32;
  for (size_t i = 0; i < length; i++) {
    crc ^= data[i];
    for (int j = 0; j < 8; j++) {
      crc = (crc & 1) ? (crc >> 1) ^ 0xEDB88320 : (crc >> 1);
    }
  }
  return ~crc;
}

WriteAheadLog::WriteAheadLog(const std::string &directory)
    : directory_(directory), current_log_id_(1) {

  std::filesystem::create_directories(directory_);
  // TODO: find highest existing log ID instead of "1"
  open_log_file(current_log_id_);
}

WriteAheadLog::~WriteAheadLog() {
  if (file_.is_open()) {
    file_.close();
  }
}

std::string WriteAheadLog::get_log_filepath(uint64_t log_id) const {
  std::ostringstream oss;
  // format as 000001.wal
  oss << directory_ << "/" << std::setfill('0') << std::setw(6) << log_id
      << ".wal";
  return oss.str();
}

void WriteAheadLog::open_log_file(uint64_t log_id) {
  std::string path = get_log_filepath(log_id);
  file_.open(path, std::ios::app | std::ios::binary);
  if (!file_.is_open()) {
    throw std::runtime_error("Failed to open WAL file: " + path);
  }
}

void WriteAheadLog::rotate_log(uint64_t new_log_id) {
  std::lock_guard<std::mutex> lock(log_mutex_);

  if (file_.is_open()) {
    file_.close();
  }

  // delete the old WAL, its data is now in an SSTable
  std::string old_path = get_log_filepath(current_log_id_);
  std::filesystem::remove(old_path);

  current_log_id_ = new_log_id;
  open_log_file(current_log_id_);
}

void WriteAheadLog::append(uint8_t op_type, const std::string &key,
                           const std::string &value, uint64_t seq_num) {
  WALRecordHeader header;
  header.op_type = op_type;
  header.seq_num = seq_num;
  header.key_len = static_cast<uint32_t>(key.size());
  header.val_len = static_cast<uint32_t>(value.size());

  uint32_t crc =
      calculate_crc32(reinterpret_cast<const uint8_t *>(&header.op_type),
                      sizeof(header.op_type));
  crc = calculate_crc32(reinterpret_cast<const uint8_t *>(&header.seq_num),
                        sizeof(header.seq_num), crc);
  crc = calculate_crc32(reinterpret_cast<const uint8_t *>(key.data()),
                        key.size(), crc);
  if (header.val_len > 0) {
    crc = calculate_crc32(reinterpret_cast<const uint8_t *>(value.data()),
                          value.size(), crc);
  }
  header.crc32 = crc;

  std::lock_guard<std::mutex> lock(log_mutex_);

  file_.write(reinterpret_cast<const char *>(&header), sizeof(header));
  file_.write(key.data(), key.size());
  if (header.val_len > 0) {
    file_.write(value.data(), value.size());
  }
  file_.flush(); // TODO: optimize with group commit
}

void WriteAheadLog::append_batch(const std::vector<BatchEntry> &batch) {
  std::lock_guard<std::mutex> lock(log_mutex_);

  for (const auto &req : batch) {
    WALRecordHeader header;
    header.op_type = req.op_type;
    header.seq_num = req.seq_num;
    header.key_len = static_cast<uint32_t>(req.key.size());
    header.val_len = static_cast<uint32_t>(req.value.size());

    // compute crc32
    uint32_t crc =
        calculate_crc32(reinterpret_cast<const uint8_t *>(&header.op_type),
                        sizeof(header.op_type));
    crc = calculate_crc32(reinterpret_cast<const uint8_t *>(&header.seq_num),
                          sizeof(header.seq_num), crc);
    crc = calculate_crc32(reinterpret_cast<const uint8_t *>(req.key.data()),
                          req.key.size(), crc);
    if (header.val_len > 0) {
      crc = calculate_crc32(reinterpret_cast<const uint8_t *>(req.value.data()),
                            req.value.size(), crc);
    }
    header.crc32 = crc;

    // write to buffer
    file_.write(reinterpret_cast<const char *>(&header), sizeof(header));
    file_.write(req.key.data(), req.key.size());
    if (header.val_len > 0)
      file_.write(req.value.data(), req.value.size());
  }

  // single physical flush for entire batch
  file_.flush();
}

void WriteAheadLog::recover(MemTable &memtable) {
  // TODO: loop over all .wal files
  std::string path = get_log_filepath(current_log_id_);
  std::ifstream infile(path, std::ios::binary);

  if (!infile.is_open())
    return;

  while (true) {
    WALRecordHeader header;
    if (!infile.read(reinterpret_cast<char *>(&header), sizeof(header)))
      break;

    std::string key(header.key_len, '\0');
    if (!infile.read(key.data(), header.key_len))
      break;

    std::string value(header.val_len, '\0');
    if (header.val_len > 0) {
      if (!infile.read(value.data(), header.val_len))
        break;
    }

    uint32_t crc =
        calculate_crc32(reinterpret_cast<const uint8_t *>(&header.op_type),
                        sizeof(header.op_type));
    crc = calculate_crc32(reinterpret_cast<const uint8_t *>(&header.seq_num),
                          sizeof(header.seq_num), crc);
    crc = calculate_crc32(reinterpret_cast<const uint8_t *>(key.data()),
                          key.size(), crc);
    if (header.val_len > 0)
      crc = calculate_crc32(reinterpret_cast<const uint8_t *>(value.data()),
                            value.size(), crc);

    if (crc != header.crc32) {
      std::cerr << "[WAL] Checksum mismatch during recovery. Stopping at "
                   "corruption point.\n";
      break;
    }

    // insert into MemTable
    // overwrite if sequence number is newer
    auto it = memtable.find(key);
    if (it == memtable.end() || it->second.seq_num < header.seq_num) {
      memtable[key] = ValueRecord{
          value, header.seq_num,
          header.op_type == 1 // true if REMOVE, i.e is tombstone
      };
    }
  }
}

} // namespace kvstore
