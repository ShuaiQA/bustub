//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>

#include "common/config.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
// 每一个页面的头部标识所需要的字节数
#define INTERNAL_PAGE_HEADER_SIZE 24
// 每一页的大小是4096减去内部页面头部的24字节，在除以MappingType(pair<K,V>的大小)获得一个页面能够存储的MappingType
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  auto ValueAt(int index) const -> ValueType;
  auto GetNextPageId(const KeyType &key, const KeyComparator &comp) -> page_id_t;
  void SetIndexKeyValue(int index, const KeyType &key, const ValueType &val);
  // 将一个节点的内部array_一般的元素,移动到另一个next中
  // 一般是对于满的节点的操作
  void Insert(const KeyType &key, const ValueType &val, const KeyComparator &comp);

  auto Split(B_PLUS_TREE_INTERNAL_PAGE_TYPE *other, const KeyComparator &comp) -> KeyType;

  auto ChangePos0Key(const KeyType &oldkey, const KeyType &newkey, const KeyComparator &comp) -> bool;

 private:
  // Flexible array member for page data.
  MappingType array_[INTERNAL_PAGE_SIZE];
};
}  // namespace bustub
