//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "include/buffer/lru_k_replacer.h"
#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <iterator>
#include <memory>
#include "../include/container/hash/extendible_hash_table.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) { num_ = 0; }

LRUKReplacer::~LRUKReplacer() {
  cache_.clear();
  cach_.clear();
  hist_.clear();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto lam = [](Node node) { return !node.replace_; };
  auto cur = std::find_if(hist_.begin(), hist_.end(), lam);
  bool ans = false;
  if (cur == hist_.end()) {
    cur = std::find_if(cach_.begin(), cach_.end(), lam);
    if (cur != cach_.end()) {  // cur find in cach_
      *frame_id = cur->frame_;
      cache_.erase(cur->frame_);
      cach_.erase(cur);
      ans = true;
      num_--;
    }
  } else {  // cur find in hist_
    *frame_id = cur->frame_;
    cache_.erase(cur->frame_);
    hist_.erase(cur);
    ans = true;
    num_--;
  }
  return ans;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "LRUKReplacerAccess out frame_id");
  }
  auto f = cache_.find(frame_id);
  if (f != cache_.end()) {  // 查找到已有的
    size_t cnt = ++f->second->cnt_;
    bool v = f->second->replace_;
    if (cnt == k_) {  // hist移动到cach
      hist_.erase(f->second);
      cach_.emplace_back(Node(frame_id, cnt, v));
      cache_[frame_id] = std::prev(cach_.end());
    } else if (cnt > k_) {  // cach_ 使用LRU
      cach_.erase(f->second);
      cach_.emplace_back(Node(frame_id, cnt, v));
      cache_[frame_id] = std::prev(cach_.end());
    }
  } else {  // 没有找到需要进行创建
            // 如果当前已经满了需要进行删除
    Node node(frame_id);
    hist_.push_back(node);
    cache_[frame_id] = std::prev(hist_.end());
    num_++;
  }
}
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "LRUKReplacerAccess out frame_id");
  }
  auto f = cache_.find(frame_id);
  if (f != cache_.end()) {
    bool v = f->second->replace_;
    if (!v && !set_evictable) {  // false false
      f->second->replace_ = true;
      num_--;
    }
    if (v && set_evictable) {
      f->second->replace_ = false;
      num_++;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "LRUKReplacerAccess out frame_id");
  }
  auto f = cache_.find(frame_id);
  if (f != cache_.end()) {
    if (f->second->replace_) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "LRUKReplacerAccess not remove");
    }
    size_t n = f->second->cnt_;
    if (n >= k_) {
      cach_.erase(f->second);
    } else {
      hist_.erase(f->second);
    }
    cache_.erase(frame_id);
    num_--;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return num_;
}

}  // namespace bustub
