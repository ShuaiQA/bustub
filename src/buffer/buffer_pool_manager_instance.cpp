//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "include/buffer/buffer_pool_manager_instance.h"
#include <cstddef>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

/**
 * 将page_id的值指向新的id也就是下一个id值，如果没有frames那么设置为空
 *
 */
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 当前的帧有剩余可以从空闲帧中进行获取相关的帧,然后进行page_table_的添加操作
  // 设置相关的pages数组中的相应的值，page_id_和pin_count_值
  frame_id_t free = GetFrame();
  if (free == -1) {
    page_id = nullptr;
    return nullptr;
  }
  *page_id = AllocatePage();
  ChangeFrameToNewPage(free, *page_id);
  return &pages_[free];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  frame_id_t cur = -1;
  if (page_table_->Find(page_id, cur)) {
    return &pages_[cur];
  }
  cur = GetFrame();
  if (cur == -1) {
    return nullptr;
  }
  ChangeFrameToNewPage(cur, page_id);
  return &pages_[cur];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t frame = -1;
  if (!page_table_->Find(page_id, frame) || pages_[frame].pin_count_ == 0) {
    return false;
  }
  pages_[frame].pin_count_--;
  if (pages_[frame].pin_count_ == 0) {
    replacer_->SetEvictable(frame, true);
  }
  pages_[frame].is_dirty_ = is_dirty;
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  frame_id_t frame = -1;
  if (page_table_->Find(page_id, frame)) {
    if (pages_[frame].is_dirty_) {
      disk_manager_->WritePage(pages_[frame].page_id_, pages_[frame].GetData());
      pages_[frame].is_dirty_ = false;
    }
  }
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].page_id_ != INVALID_PAGE_ID) {
      FlushPgImp(pages_[i].page_id_);
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  frame_id_t cur = -1;
  if (!page_table_->Find(page_id, cur)) {
    return true;
  }
  if (pages_[cur].pin_count_ != 0) {
    return false;
  }
  replacer_->Remove(cur);
  page_table_->Remove(page_id);
  free_list_.push_back(cur);
  FlushPgImp(page_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
