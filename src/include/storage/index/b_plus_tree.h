//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <vector>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  // 对于一个内部节点来说,他的value应该是下一个页面
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  // 对于一个叶子节点来说,他的value应该是指向真实数据的指针
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  // 由此引申出来内部节点和叶子节点的max_size是不一样的,因为pair的大小是不一样的

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key,与Search进行匹配
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

 private:
  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  auto Search(page_id_t root, const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr)
      -> bool;

  auto CreateNewLeafPage(page_id_t *page_id, page_id_t parent = INVALID_PAGE_ID, page_id_t next_page = INVALID_PAGE_ID)
      -> bool;
  auto CreateNewInternalPage(page_id_t *page_id, page_id_t parent = INVALID_PAGE_ID) -> bool;
  void DfsSplit(page_id_t cur, Transaction *transaction);

  // 该上锁比较粗颗粒
  void DfsChangePos0(page_id_t father, const KeyType &oldkey, const KeyType &newkey, Transaction *transaction);
  // 根据传入的key找到应该存储的 page_id_t 如果当前的page_id_t里面没有进行插入
  auto FindShouldLocalPage(const KeyType &key, Transaction *transaction = nullptr) -> page_id_t;
  // 根据传入的叶子节点,返回当前叶子节点的左兄弟节点,没有返回nullptr
  auto FindLeafLeafData(LeafPage *cur) -> page_id_t;

  // 根据传入的内部节点返回内部节点的左右兄弟节点
  auto FindInternalLeafData(InternalPage *cur) -> InternalPage *;
  auto FindInternalRightData(InternalPage *cur) -> InternalPage *;

  // 根据传入的当前节点判断当前的内部节点是否需要进行借或者合并,递归的向上进行遍历
  void DfsShouldCombine(page_id_t c, Transaction *transaction);

  // 获取第一个叶子和获取最后一个叶子节点
  auto GetFirstLeafData(page_id_t root) -> LeafPage *;
  auto GetLastLeafData(page_id_t root) -> LeafPage *;

  // 删除queue中的所有page的锁
  void DeleteUnlock(int type, Transaction *transaction);
  // 上锁操作
  void AddSomeLock(int type, Transaction *transaction, BPlusTreePage *cur);

  // 当前的变量保存这根节点的页面、缓冲池的指针、一个比较器、叶节点的最大容量、内部节点的最大容量
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
};

}  // namespace bustub
