//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <limits>
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <utility>
#include <vector>
#include "../common/config.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

class Node {
 public:
  frame_id_t frame_id_;
  // 用于替换算法
  size_t cnt_;
  // 标记是否可以被替换
  bool is_replace_;  // false 可以被取代
  std::shared_ptr<Node> next_, pre_;
  Node(frame_id_t frame_id, size_t cnt, bool is_replace) : frame_id_(frame_id), cnt_(cnt), is_replace_(is_replace) {}
};
class DoubleList {
 public:
  std::shared_ptr<Node> head_, tail_;  // head_存放下一个删除的节点,tail_存放更新的节点的位置
  DoubleList() {
    // 内存泄漏
    head_ = std::make_shared<Node>(0, 0, false);
    tail_ = std::make_shared<Node>(0, 0, false);
    head_->next_ = tail_;
    tail_->pre_ = head_;
  }
  ~DoubleList() {
    auto c = head_->next_;
    while (c != tail_) {
      auto b = c->next_;
      RemoveNode(c);
      c = b;
    }
    head_->next_ = nullptr;
    tail_->pre_ = nullptr;
  }
  void RemoveNode(std::shared_ptr<Node> &node) {
    node->pre_->next_ = node->next_;
    node->next_->pre_ = node->pre_;
    node->pre_ = nullptr;
    node->next_ = nullptr;
  }
  void AddNodeToTail(std::shared_ptr<Node> &node) {  // 更新的时候需要
    auto temp = tail_->pre_;
    temp->next_ = node;
    node->pre_ = temp;
    tail_->pre_ = node;
    node->next_ = tail_;
  }
  // 删除一个节点，如果没有返回空
  auto DeleteFirstEvictableNode() -> std::shared_ptr<Node> {
    auto temp = GetFirstEvictableNode();
    if (temp != nullptr) {
      RemoveNode(temp);
      return temp;
    }
    return nullptr;
  }
  // 返回一个可以最开始的删除的节点
  auto GetFirstEvictableNode() -> std::shared_ptr<Node> {
    auto temp = head_->next_;
    while (temp != tail_) {
      if (!temp->is_replace_) {
        break;
      }
      temp = temp->next_;
    }
    return temp == tail_ ? nullptr : temp;
  }
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k hist_orical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k hist_orical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access hist_ory.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access hist_ory if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access hist_ory.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  size_t replacer_size_;  // 标记当前最大包含可驱逐和不可驱逐的数目
  size_t k_;              // 如果访问次数大于等于k_将hist_中的数据放到cach_中
  std::mutex latch_;
  // 历史队列双链表存储没有到达K的cache_
  std::unordered_map<frame_id_t, std::shared_ptr<Node>> cache_;  // 保存可驱逐和不可驱逐的大小
  // 当前的cache_队列存储到达K的cach
  std::shared_ptr<DoubleList> hist_, cach_;
  size_t num_;  // 可被驱逐的帧的数目

  // 将一个node节点添加到相关的map和链表中
  void AddNode(frame_id_t frame_id, std::shared_ptr<Node> &node) {
    if (node->cnt_ < k_) {
      hist_->AddNodeToTail(node);
    } else {
      cach_->AddNodeToTail(node);
    }
    cache_[frame_id] = node;
    num_++;
  }

  auto DeleteNode(frame_id_t frame_id) -> std::shared_ptr<Node> {
    auto cur = cache_[frame_id];
    if (cur->cnt_ < k_) {
      hist_->RemoveNode(cur);
    } else {
      cach_->RemoveNode(cur);
    }
    cache_.erase(frame_id);
    num_--;
    return cur;
  }
};

}  // namespace bustub
