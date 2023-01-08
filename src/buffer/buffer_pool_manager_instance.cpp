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
#include "common/logger.h"
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
  LOG_INFO("pool_size is [%d]", (int)pool_size_);
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
  std::scoped_lock<std::mutex> lock(latch_);
  // 获取相关的空闲的帧
  frame_id_t free = GetFrame();
  // 没有找到设置返回值和page_id
  if (free == -1) {
    page_id = nullptr;
    return nullptr;
  }
  // 找到相关的帧，进行分配页面
  *page_id = AllocatePage();
  ChangeFrameToNewPage(free, *page_id);
  return &pages_[free];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t cur = -1;
  // 如果找到相关的对应页面需要进行相关的LRU和pin_count_设置
  if (page_table_->Find(page_id, cur)) {
    pages_[cur].pin_count_++;
    replacer_->RecordAccess(cur);
    replacer_->SetEvictable(cur, false);
    return &pages_[cur];
  }
  // 如果没有找到,那么需要进行更新操作,找到一个新的帧进行替换
  cur = GetFrame();
  if (cur == -1) {  // 没有找到新的帧
    LOG_INFO("内存不够等待缓冲区释放");
    Debug();
    return nullptr;
  }
  // 找到新的帧进行初始话的过程
  ChangeFrameToNewPage(cur, page_id);
  return &pages_[cur];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame = -1;
  // 如果不再page_table_中或者是pin_count_==0直接返回false
  if (!page_table_->Find(page_id, frame)) {  // 没有找到直接返回false
    return false;
  }
  // 修改相关的帧的脏位
  if (is_dirty) {
    pages_[frame].is_dirty_ = is_dirty;
  }
  if (pages_[frame].pin_count_ == 0) {
    return false;
  }
  // 对相关的frame的pin_count_-1
  pages_[frame].pin_count_--;
  // 如果pin_count_==0,可以将replacer_的设置为相关的true,可以进行删除
  if (pages_[frame].pin_count_ == 0) {
    replacer_->SetEvictable(frame, true);
  }
  // Debug();
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
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t cur = -1;
  // 不在page_table_中进行不用删除，直接返回true
  if (!page_table_->Find(page_id, cur)) {
    return true;
  }
  // 当前的pin_count_ != 0不应该进行删除操作
  if (pages_[cur].pin_count_ != 0) {
    return false;
  }
  // 删除page_table_和LRU,将帧进行添加到free_list_
  replacer_->Remove(cur);
  page_table_->Remove(page_id);
  free_list_.push_back(cur);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
