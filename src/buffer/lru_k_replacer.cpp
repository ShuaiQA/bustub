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
#include <cstdlib>
#include <memory>
#include "../include/container/hash/extendible_hash_table.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k), num_(0) {
  // 内存泄漏
  hist_ = std::make_shared<DoubleList>();
  cach_ = std::make_shared<DoubleList>();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  Lock w(&latch_);
  auto cur = hist_->DeleteFirstEvictableNode();
  if (cur == nullptr) {
    cur = cach_->DeleteFirstEvictableNode();
    if (cur == nullptr) {
      return false;
    }
  }
  cache_.erase(cur->frame_id_);
  *frame_id = cur->frame_id_;
  num_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  Lock w(&latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "LRUKReplacerAccess out frame_id");
  }
  auto f = cache_.find(frame_id);  // 查看当前是否存在如果存在的话增加，如果没有存在那么增加
  if (f == cache_.end()) {         // 没有找到对应的页面,创建一个页面到hist__tail_里面
    std::shared_ptr<Node> node = std::make_shared<Node>(frame_id, 1, false);
    if (cache_.size() == replacer_size_) {  // 如果当前已经满了,进行删除操作
      frame_id_t *rep = nullptr;
      Evict(rep);
    }
    AddNode(frame_id, node);
  } else {  // 找到当前的节点
    auto cur = DeleteNode(frame_id);
    cur->cnt_++;
    AddNode(frame_id, cur);
  }
}
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  Lock w(&latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "LRUKReplacerAccess out frame_id");
  }
  auto f = cache_.find(frame_id);
  if (f != cache_.end()) {                          // 能够找到相关的值
    if (set_evictable && f->second->is_replace_) {  // 设置为可以替换的,当前是不可以取代的
      f->second->is_replace_ = false;
      num_++;
    }
    if (!set_evictable && !f->second->is_replace_) {  // 设置为不可以替换的,当前是可以替换的
      f->second->is_replace_ = true;
      num_--;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  Lock w(&latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "LRUKReplacerAccess out frame_id");
  }
  auto f = cache_.find(frame_id);
  if (f != cache_.end()) {
    if (f->second->is_replace_) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "LRUKReplacerAccess remove a pin frame");
    }
    DeleteNode(frame_id);
  }
}

auto LRUKReplacer::Size() -> size_t {
  Lock w(&latch_);
  return num_;
}

}  // namespace bustub
