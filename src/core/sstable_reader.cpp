#include "kvstore/sstable_reader.hpp"
#include <algorithm>
#include <iostream>

namespace kvstore {

SSTableReader::SSTableReader(const std::string &filepath)
    : filepath_(filepath) {
  file_.open(filepath_, std::ios::in | std::ios::binary);
  if (!file_.is_open()) {
    throw std::runtime_error("Failed to open SSTable for reading: " +
                             filepath_);
  }
  load_index();
}

SSTableReader::~SSTableReader() {
  if (file_.is_open()) {
    file_.close();
  }
}

void SSTableReader::load_index() {
  // jump to EOF - 8 bytes to find the footer
  file_.seekg(-8, std::ios::end);
  file_.read(reinterpret_cast<char *>(&index_offset_), sizeof(index_offset_));

  // jump to where the index block begins
  file_.seekg(index_offset_, std::ios::beg);

  // read the number of index entries
  uint32_t num_entries;
  file_.read(reinterpret_cast<char *>(&num_entries), sizeof(num_entries));

  // load sparse index into RAM
  index_.reserve(num_entries);
  for (uint32_t i = 0; i < num_entries; ++i) {
    uint32_t key_len;
    file_.read(reinterpret_cast<char *>(&key_len), sizeof(key_len));

    std::string key(key_len, '\0');
    file_.read(key.data(), key_len);

    uint64_t offset;
    file_.read(reinterpret_cast<char *>(&offset), sizeof(offset));

    index_.push_back({key, offset});
  }

  std::cout << "[SSTableReader] Loaded " << num_entries << " index blocks from "
            << filepath_ << "\n";
}

std::optional<std::string> SSTableReader::get(const std::string &key) {
  if (index_.empty())
    return std::nullopt;

  // binary search to find the block that might contain the key
  auto it =
      std::upper_bound(index_.begin(), index_.end(), key,
                       [](const std::string &target, const IndexEntry &entry) {
                         return target < entry.key;
                       });

  if (it == index_.begin()) {
    return std::nullopt;
  }

  // step back one entry to get the block that should contain the key
  --it;
  uint64_t search_offset = it->offset;

  // offset to stop reading at
  uint64_t stop_offset =
      (std::next(it) != index_.end()) ? std::next(it)->offset : index_offset_;

  std::lock_guard<std::mutex> lock(read_mutex_);

  // jump to data block
  file_.seekg(search_offset, std::ios::beg);

  // linear scan through this block
  while (static_cast<uint64_t>(file_.tellg()) < stop_offset) {
    SSTRecordHeader header;
    if (!file_.read(reinterpret_cast<char *>(&header), sizeof(header)))
      break;

    std::string current_key(header.key_len, '\0');
    file_.read(current_key.data(), header.key_len);

    if (current_key > key)
      break;

    if (current_key == key) {
      // check if deleted
      if (header.is_tombstone) {
        return std::nullopt;
      }

      std::string value(header.val_len, '\0');
      file_.read(value.data(), header.val_len);
      return value;
    } else {
      // skip the value payload and move to the next record
      file_.seekg(header.val_len, std::ios::cur);
    }
  }

  return std::nullopt;
}

SSTableIterator::SSTableIterator(const std::string &filepath, size_t file_idx)
    : file_idx_(file_idx), valid_(true) {
  file_.open(filepath, std::ios::in | std::ios::binary);

  // find where the index starts to know when to stop reading
  file_.seekg(-8, std::ios::end);
  file_.read(reinterpret_cast<char *>(&index_offset_), sizeof(index_offset_));

  file_.seekg(0, std::ios::beg);
  next(); // load the first record
}

void SSTableIterator::next() {
  if (static_cast<uint64_t>(file_.tellg()) >= index_offset_) {
    valid_ = false;
    return;
  }

  SSTRecordHeader header;
  if (!file_.read(reinterpret_cast<char *>(&header), sizeof(header))) {
    valid_ = false;
    return;
  }

  current_record_.seq_num = header.seq_num;
  current_record_.is_tombstone = (header.is_tombstone == 1);
  current_record_.file_index = file_idx_;

  current_record_.key.resize(header.key_len);
  file_.read(current_record_.key.data(), header.key_len);

  current_record_.value.resize(header.val_len);
  if (header.val_len > 0) {
    file_.read(current_record_.value.data(), header.val_len);
  }
}

bool SSTableIterator::is_valid() const { return valid_; }
MergedRecord SSTableIterator::current() const { return current_record_; }

} // namespace kvstore
