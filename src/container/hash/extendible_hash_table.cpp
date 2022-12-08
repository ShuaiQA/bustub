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
#include <strings.h>
#include <unistd.h>
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <iterator>
#include <list>
#include <memory>
#include <utility>
#include "common/logger.h"
#include "parser/parser.hpp"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  global_depth_ = 0;
  num_buckets_ = 1;
  dir_ = std::vector<std::shared_ptr<Bucket>>(1, std::make_shared<Bucket>(bucket_size));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  // 根据当前的全局depth,获取hash的后全局depth位
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
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t pos = IndexOf(key);
  return dir_[pos]->Remove(key);
}

/*
 * 获取当前的index，判断能不能插入对应的index(进行while判断)
 * 如果在vector中有4个Bucket同时指向一个地方,扩展的时候应该怎么进行扩展?查看最后的指针的修改
 */
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);  // 获取vector中的下标
  // 查看是否存在key,如果存在直接进行更新
  V val;
  if (dir_[index]->Find(key, val)) {  // 已经存在直接调用桶函数进行修改
    dir_[index]->Insert(key, value);
    return;
  }
  // 当前不存在查看当前的桶是否是满的
  while (dir_[index]->IsFull()) {  // 满的话返回true,执行下面的扩充函数
    int local_dep = dir_[index]->GetDepth();
    if (global_depth_ == local_dep) {  // 进行dir_*2倍扩充,使指针指向相同的地方
      decltype(dir_.size()) cnt = dir_.size();
      dir_.reserve(cnt * 2);
      std::copy_n(dir_.begin(), cnt, std::back_inserter(dir_));
      global_depth_++;
    }
    auto b0 = std::make_shared<Bucket>(bucket_size_, local_dep + 1);
    auto b1 = std::make_shared<Bucket>(bucket_size_, local_dep + 1);
    int local_mask = 1 << local_dep;
    for (const auto &[k, v] : dir_[index]->GetItems()) {
      size_t cur = std::hash<K>()(k) & local_mask;  // 类似一个是0000,一个是1000
      if (static_cast<bool>(cur)) {
        b1->Insert(k, v);
      } else {
        b0->Insert(k, v);
      }
    }
    // 对于相关的vector的指针，我们需要进行调整为新的b0和b1
    for (size_t i = (std::hash<K>()(key) & (local_mask - 1)); i < dir_.size(); i += local_mask) {
      if (static_cast<bool>(i & local_mask)) {
        dir_[i] = b1;
      } else {
        dir_[i] = b0;
      }
    }
    num_buckets_++;
    index = IndexOf(key);
  }
  dir_[index]->Insert(key, value);
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
  auto begin = list_.begin();
  bool v = false;
  while (begin != list_.end()) {
    K first = begin->first;
    if (first == key) {
      break;
    }
    begin++;
  }
  if (begin == list_.end()) {  // 没有找到当前的值,进行添加
    if (!IsFull()) {
      list_.push_back(std::pair<K, V>(key, value));
      v = true;
    }
  } else {  // 查找到当前的值进行修改
    begin->second = value;
    v = true;
  }
  return v;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
