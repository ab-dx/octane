#include "kvstore/sstable.hpp"
#include <iostream>

namespace kvstore {

// index a key every 4KB
constexpr size_t INDEX_BLOCK_SIZE = 4096;

SSTableWriter::SSTableWriter(const std::string &filepath)
    : filepath_(filepath), bytes_since_last_index_(0) {

  // open in binary mode, truncating any existing file
  file_.open(filepath_, std::ios::out | std::ios::binary | std::ios::trunc);
  if (!file_.is_open()) {
    throw std::runtime_error("Failed to open SSTable for writing: " +
                             filepath_);
  }
}

SSTableWriter::~SSTableWriter() {
  if (file_.is_open()) {
    file_.close();
  }
}

void SSTableWriter::write_memtable(const MemTable &memtable) {
  std::cout << "[SSTableWriter] Flushing " << memtable.size() << " entries to "
            << filepath_ << "\n";

  for (auto it = memtable.begin(); it != memtable.end(); ++it) {
    const std::string &key = it->first;
    const ValueRecord &record = it->second;

    uint64_t current_offset = static_cast<uint64_t>(file_.tellp());

    // check if new index entry needed
    if (index_.empty() || bytes_since_last_index_ >= INDEX_BLOCK_SIZE) {
      index_.push_back({key, current_offset});
      bytes_since_last_index_ = 0;
    }

    // write record header
    SSTRecordHeader header;
    header.seq_num = record.seq_num;
    header.key_len = static_cast<uint32_t>(key.size());
    header.val_len = static_cast<uint32_t>(record.value.size());
    header.is_tombstone = record.is_tombstone ? 1 : 0;

    file_.write(reinterpret_cast<const char *>(&header), sizeof(header));

    // write key and value payloads
    file_.write(key.data(), key.size());
    if (!record.is_tombstone) {
      file_.write(record.value.data(), record.value.size());
    }

    bytes_since_last_index_ +=
        sizeof(header) + key.size() + record.value.size();
  }

  // write index block
  uint64_t index_offset = static_cast<uint64_t>(file_.tellp());
  write_index_block();

  // 8 byte footer pointing to the index block
  write_footer(index_offset);

  file_.flush();
  std::cout << "[SSTableWriter] Flush complete. Wrote " << index_.size()
            << " index blocks.\n";
}

void SSTableWriter::write_index_block() {
  // write total number of index entries
  uint32_t num_entries = static_cast<uint32_t>(index_.size());
  file_.write(reinterpret_cast<const char *>(&num_entries),
              sizeof(num_entries));

  // serialize each index entry [KeyLen][Key][Offset]
  for (const auto &entry : index_) {
    uint32_t key_len = static_cast<uint32_t>(entry.key.size());
    file_.write(reinterpret_cast<const char *>(&key_len), sizeof(key_len));
    file_.write(entry.key.data(), entry.key.size());
    file_.write(reinterpret_cast<const char *>(&entry.offset),
                sizeof(entry.offset));
  }
}

void SSTableWriter::write_footer(uint64_t index_offset) {
  // last 8 bytes of the file
  // jump to EOF - 8 bytes to find this
  file_.write(reinterpret_cast<const char *>(&index_offset),
              sizeof(index_offset));
}

} // namespace kvstore
