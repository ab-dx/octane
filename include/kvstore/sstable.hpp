#pragma once

#include "kvstore/wal.hpp"
#include <cstdint>
#include <fstream>
#include <string>
#include <vector>

namespace kvstore {

// entry in SSTable's index block
struct IndexEntry {
  std::string key;
  uint64_t offset; // byte offset where this key's block starts
};

#pragma pack(push, 1)
struct SSTRecordHeader {
  uint64_t seq_num;
  uint32_t key_len;
  uint32_t val_len;
  uint8_t is_tombstone; // 1 if deleted, 0 if active
};
#pragma pack(pop)

class SSTableWriter {
public:
  explicit SSTableWriter(const std::string &filepath);
  ~SSTableWriter();

  SSTableWriter(const SSTableWriter &) = delete;
  SSTableWriter &operator=(const SSTableWriter &) = delete;

  // serializes MemTable to disk
  void write_memtable(const MemTable &memtable);

private:
  void write_index_block();
  void write_footer(uint64_t index_offset);

  std::string filepath_;
  std::ofstream file_;

  // in-memory index built as we write data blocks
  std::vector<IndexEntry> index_;

  size_t bytes_since_last_index_;
};

} // namespace kvstore
