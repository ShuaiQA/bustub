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

#include <algorithm>
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
#include "nodes/nodes.hpp"

namespace bustub {

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

  class Node {
   public:
    size_t cnt_;
    bool replace_;
    frame_id_t frame_;
    explicit Node(frame_id_t frame, size_t cnt = 1, bool replace = false)
        : cnt_(cnt), replace_(replace), frame_(frame) {}
  };

  void Debug() {
    for (const auto [k, v] : cache_) {
      LOG_INFO("frame [%d], Node(%d,%d,%d)\n", k, v->frame_, static_cast<int>(v->cnt_), v->replace_);
    }
    for (auto node : hist_) {
      LOG_INFO("Node(%d,%d,%d)\n", node.frame_, static_cast<int>(node.cnt_), node.replace_);
    }
    for (auto node : cach_) {
      LOG_INFO("Node(%d,%d,%d)\n", node.frame_, static_cast<int>(node.cnt_), node.replace_);
    }
  }

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  size_t replacer_size_;  // 标记当前最大包含可驱逐和不可驱逐的数目
  size_t k_;              // 如果访问次数大于等于k_将hist_中的数据放到cach_中
  std::mutex latch_;
  // 历史队列双链表存储没有到达K的cache_
  std::unordered_map<frame_id_t, std::list<Node>::iterator> cache_;
  std::list<Node> hist_, cach_;  // hist_中的元素使用FIFO进行删除begin,cach_元素使用LRU进行删除begin
  size_t num_;                   // 可被驱逐的帧的数目
};

}  // namespace bustub
