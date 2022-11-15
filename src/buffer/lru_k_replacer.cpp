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

#include "../include/buffer/lru_k_replacer.h"
#include <algorithm>
#include "../include/container/hash/extendible_hash_table.h"
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  hist_ = std::make_shared<DoubleList>();
  cach_ = std::make_shared<DoubleList>();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  Lock w(&latch_);
  bool ans = false;
  if (hist_->size_ != 0) {
    auto node = hist_->DeleteHeadNode();
    *frame_id = node->p_.first;
    cache_.erase(cache_.find(node->p_.first));
    ans = true;
  } else if (cach_->size_ != 0) {
    auto node = cach_->DeleteHeadNode();
    *frame_id = node->p_.first;
    cache_.erase(cache_.find(node->p_.first));
    ans = true;
  }
  return ans;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  Lock w(&latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "LRUKReplacerAccess out frame_id");
  }
  auto f = cache_.find(frame_id);
  if (f == cache_.end()) {  // 没有找到对应的页面,创建一个页面到hist__tail_里面
    auto node = std::make_shared<Node>(std::pair<frame_id_t, size_t>(frame_id, 1));
    if (cache_.size() == replacer_size_) {  // 需要进行删除
      if (hist_->size_ != 0) {              // 满了进行删除其中的一个
        auto del = hist_->DeleteHeadNode();
        cache_.erase(cache_.find(del->p_.second));
      } else {
        auto del = cach_->DeleteHeadNode();
        cache_.erase(cache_.find(del->p_.second));
      }
    }
    hist_->AddNodeToTail(node);  // 进行添加进去
    cache_[frame_id] = node;
  } else {
    // 找到对应的map中的Node,获取当前的Node修改cnt,如果当前的cnt==k-1,从hist_中删除,将其放到cache__tail_里,
    // 如果当前cnt<k-1,修改cnt,从hist_双链表原地方进行删除,将其放到hist__tail_里
    auto node = f->second;            // 找到当前的节点
    if (node->p_.second == k_ - 1) {  // 如果当前的节点正好是k-1,将其从hist_放到cach中
      hist_->RemoveNode(node);
      cach_->AddNodeToTail(node);
      node->p_.second++;
    } else if (node->p_.second < k_ - 1) {  // 小于某一个边界值,将其移动到hist_的tail_里面
      hist_->RemoveNode(node);
      hist_->AddNodeToTail(node);
      node->p_.second++;
    } else if (node->p_.second >= k_ && node->p_.second < k_ * 2) {
      // 处于cach中,进行放到尾部
      cach_->RemoveNode(node);
      cach_->AddNodeToTail(node);
      node->p_.second++;
    } else {  // 处在cache_中并且 == replacer_size_*2, 那么将其更新到cach的尾部,重置p_.second == replacer_size_
      cach_->RemoveNode(node);
      cach_->AddNodeToTail(node);
      node->p_.second = replacer_size_;
    }
  }
}
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  Lock w(&latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "LRUKReplacerAccess out frame_id");
  }
  auto f = cache_.find(frame_id);
  if (f == cache_.end() && set_evictable) {  // 没有找到对应的页面,创建一个页面到hist__tail_里面
    auto node = std::make_shared<Node>(std::pair<frame_id_t, size_t>(frame_id, 1));
    if (cache_.size() == replacer_size_) {  // 需要进行删除
      if (hist_->size_ != 0) {              // 满了进行删除其中的一个
        hist_->DeleteHeadNode();
      } else {
        cach_->DeleteHeadNode();
      }
    }
    hist_->AddNodeToTail(node);  // 进行添加进去
    cache_[frame_id] = node;
  }
  if (f != cache_.end() && !set_evictable) {
    if (f->second->p_.second >= k_) {
      cach_->RemoveNode(f->second);
    } else {
      hist_->RemoveNode(f->second);
    }
    cache_.erase(f);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  Lock w(&latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "LRUKReplacerAccess out frame_id");
  }
  auto f = cache_.find(frame_id);
  if (f != cache_.end()) {
    if (f->second->p_.second >= k_) {
      cach_->RemoveNode(f->second);
    } else {
      hist_->RemoveNode(f->second);
    }
    cache_.erase(f);
  }
}

auto LRUKReplacer::Size() -> size_t {
  Lock w(&latch_);
  return cache_.size();
}

}  // namespace bustub
