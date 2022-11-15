//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/hash/extendible_hash_table.h"
#include <unistd.h>
#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>
#include "common/logger.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_ = std::vector<std::shared_ptr<Bucket>>(1, std::make_shared<Bucket>(bucket_size));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  Lock w(&latch_);
  auto index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  Lock w(&latch_);
  return dir_[IndexOf(key)]->Remove(key);
}

/* 获取当前的index，判断能不能插入对应的index(进行while判断)
如果不能插入,将index的内容赋值给temp,然后将index和other进行设置为空
将temp里面的内容继续调用当前的函数进行递归的插入
已知当前的index如果超出大小的话,如何找到另一个other
如果在vector中有4个Bucket同时指向一个地方,扩展的时候应该怎么进行扩展 */
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  latch_.lock();
  int index = IndexOf(key);
  while (!dir_[index]->Insert(key, value)) {  // 看看能不能插入
    latch_.unlock();
    auto cur = dir_[index];                             // temp 原有的key-value放到cur中
    if (dir_[index]->GetDepth() == GetGlobalDepth()) {  // dir扩展2倍
      global_depth_++;
      std::vector<std::shared_ptr<Bucket>> cur(dir_.size() * 2);
      auto min = copy(dir_.begin(), dir_.end(), cur.begin());
      copy(dir_.begin(), dir_.end(), min);
      dir_ = cur;
    }
    for (decltype(dir_.size()) a = 0; a < dir_.size(); a++) {
      if (dir_[a] == cur) {
        dir_[a] = std::make_shared<Bucket>(bucket_size_, cur->GetDepth() + 1);
      }
    }
    auto fill_list = cur->GetItems();
    // 对当前的list进行划分
    auto begin = fill_list.begin();
    while (begin != fill_list.end()) {  // 当前的key和value应该调用本函数
      Insert(begin->first, begin->second);
      begin++;
    }
    num_buckets_++;
    index = IndexOf(key);
    latch_.lock();
  }
  latch_.unlock();
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto begin = list_.begin();
  while (begin != list_.end()) {
    K first = begin->first;
    if (first == key) {
      value = begin->second;
      break;
    }
    begin++;
  }
  return begin != list_.end();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto begin = list_.begin();
  while (begin != list_.end()) {
    K first = begin->first;
    if (first == key) {
      break;
    }
    begin++;
  }
  if (begin == list_.end()) {
    return false;
  }
  list_.erase(begin);
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {
    return false;
  }
  auto begin = list_.begin();
  while (begin != list_.end()) {
    K first = begin->first;
    if (first == key) {
      break;
    }
    begin++;
  }
  if (begin == list_.end()) {
    list_.push_back(std::pair<K, V>(key, value));
  } else {
    begin->second = value;
  }
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
