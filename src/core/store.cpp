#include "kvstore/store.hpp"
#include "kvstore/sstable.hpp"
#include <algorithm>
#include <filesystem>
#include <iostream>
#include <queue>
#include <vector>

namespace kvstore {

// 4MB MemTable threshold size
constexpr size_t MEMTABLE_FLUSH_THRESHOLD = 4 * 1024 * 1024;

KVStore::KVStore(const std::string &directory)
    : directory_(directory), estimated_memtable_size_(0), sequence_number_(0),
      current_log_id_(1), running_(true) {

  wal_ = std::make_unique<WriteAheadLog>(directory_);

  // stream the WAL directly into the MemTable
  wal_->recover(memtable_);

  // restore the sequence number and calculate rough size
  for (const auto &[k, v] : memtable_) {
    if (v.seq_num > sequence_number_) {
      sequence_number_ = v.seq_num;
    }
    estimated_memtable_size_ +=
        k.size() + v.value.size() + 16; // 16 bytes for overhead

    // start the background compactor
    compaction_thread_ = std::thread(&KVStore::compaction_worker, this);
  }

  std::cout << "[KVStore] Recovered MemTable with " << memtable_.size()
            << " entries. Sequence Number at " << sequence_number_ << "\n";
}

KVStore::~KVStore() {
  running_ = false;
  if (compaction_thread_.joinable()) {
    compaction_thread_.join();
  }
}

std::optional<std::string> KVStore::get(const std::string &key) const {
  std::shared_lock<std::shared_mutex> lock(rw_mutex_);

  auto it = memtable_.find(key);
  if (it != memtable_.end()) {
    // if the key exists but is a tombstone, it is deleted
    if (it->second.is_tombstone) {
      return std::nullopt;
    }
    return it->second.value;
  }

  // if not in the MemTable, search the SSTables
  for (auto it = sstables_.rbegin(); it != sstables_.rend(); ++it) {
    auto val = (*it)->get(key);
    if (val) {
      return val;
    }
  }

  return std::nullopt;
}

void KVStore::set(const std::string &key, const std::string &value) {
  uint64_t seq = ++sequence_number_;

  std::unique_lock<std::shared_mutex> lock(rw_mutex_);

  wal_->append(0, key, value, seq);

  memtable_[key] = ValueRecord{value, seq, false};
  estimated_memtable_size_ += key.size() + value.size() + 16;

  check_and_flush();
}

bool KVStore::remove(const std::string &key) {
  uint64_t seq = ++sequence_number_;

  std::unique_lock<std::shared_mutex> lock(rw_mutex_);

  wal_->append(1, key, "", seq);

  // set as tombstone
  memtable_[key] = ValueRecord{"", seq, true};
  estimated_memtable_size_ += key.size() + 16;

  check_and_flush();

  return true;
}

void KVStore::check_and_flush() {
  if (estimated_memtable_size_ >= MEMTABLE_FLUSH_THRESHOLD) {
    flush_memtable_to_sstable();
  }
}

void KVStore::flush_memtable_to_sstable() {
  std::cout << "\n[KVStore] MemTable size (" << estimated_memtable_size_
            << " bytes) reached threshold. Initiating flush to SSTable...\n";

  std::string sst_filename =
      directory_ + "/0_" + std::to_string(current_log_id_) + ".sst";

  SSTableWriter writer(sst_filename);
  writer.write_memtable(memtable_);

  // clear the active MemTable
  memtable_.clear();
  estimated_memtable_size_ = 0;

  // rotate the WAL
  current_log_id_++;
  wal_->rotate_log(current_log_id_);

  std::cout << "[KVStore] Flush complete. WAL rotated to log ID "
            << current_log_id_ << "\n";
}

struct MergeComparator {
  bool operator()(const MergedRecord &a, const MergedRecord &b) const {
    if (a.key == b.key) {
      // for identical keys, need largest sequence number at the top
      return a.seq_num < b.seq_num;
    }
    // smallest key alphabetically at the top
    return a.key > b.key;
  }
};

std::vector<std::string> KVStore::get_all_sstable_filepaths() const {
  std::vector<std::string> files;

  // scan directory for any file ending in .sst
  for (const auto &entry : std::filesystem::directory_iterator(directory_)) {
    if (entry.path().extension() == ".sst") {
      files.push_back(entry.path().string());
    }
  }

  std::sort(files.begin(), files.end());

  return files;
}

// background compaction worker
void KVStore::compaction_worker() {
  while (running_) {
    // wake up every 10 seconds
    std::this_thread::sleep_for(std::chrono::seconds(10));

    if (!running_)
      break;

    auto files = get_all_sstable_filepaths();

    // if more than 4 tables, compact
    if (files.size() >= 4) {
      perform_major_compaction();
    }
  }
}

void KVStore::perform_major_compaction() {
  std::vector<std::string> target_files = get_all_sstable_filepaths();
  if (target_files.size() < 2)
    return; // nothing to merge

  std::cout << "[Compaction] Starting Major Compaction on "
            << target_files.size() << " files...\n";

  // init iterators and min-heap
  std::priority_queue<MergedRecord, std::vector<MergedRecord>, MergeComparator>
      min_heap;
  std::vector<SSTableIterator> iterators;

  for (size_t i = 0; i < target_files.size(); ++i) {
    iterators.emplace_back(target_files[i], i);
    if (iterators.back().is_valid()) {
      min_heap.push(iterators.back().current());
    }
  }

  // prepare new compacted file
  std::string compacted_filepath =
      directory_ + "/compacted_" + std::to_string(current_log_id_) + ".sst";
  SSTableWriter writer(compacted_filepath);
  MemTable compaction_buffer;

  std::string last_processed_key = "";

  // K way merge loop
  while (!min_heap.empty()) {
    MergedRecord top = min_heap.top();
    min_heap.pop();

    iterators[top.file_index].next();
    if (iterators[top.file_index].is_valid()) {
      min_heap.push(iterators[top.file_index].current());
    }

    if (top.key == last_processed_key) {
      continue;
    }
    last_processed_key = top.key;

    // drop tombstones
    if (top.is_tombstone) {
      continue;
    }

    compaction_buffer[top.key] = ValueRecord{top.value, top.seq_num, false};

    // periodically flush the buffer to disk
    if (compaction_buffer.size() >= 10000) {
      writer.write_memtable(compaction_buffer);

      compaction_buffer.clear();
    }
  }

  // flush remaining
  if (!compaction_buffer.empty())
    writer.write_memtable(compaction_buffer);

  // atomic file swap
  {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);

    // clear active readers
    sstables_.clear();

    // delete old fragmented files
    for (const auto &file : target_files) {
      std::filesystem::remove(file);
    }

    // load new compacted file into the reader list
    sstables_.push_back(std::make_unique<SSTableReader>(compacted_filepath));
  }

  std::cout << "[Compaction] Finished. Wrote cleanly to " << compacted_filepath
            << "\n";
}

} // namespace kvstore
