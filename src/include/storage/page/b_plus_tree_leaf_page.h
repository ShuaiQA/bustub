//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <pthread.h>
#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
// 多出一个next_page_id
#define LEAF_PAGE_HEADER_SIZE 28
#define LEAF_PAGE_SIZE ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 28 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * | ParentPageId (4) | PageId (4) | NextPageId (4)
 *  -----------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // After creating a new leaf page from buffer pool, must call initialize
  // method to set default values
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = LEAF_PAGE_SIZE);
  // helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;
  auto GetKeyAndValue(int index) const -> const MappingType &;
  auto Insert(const KeyType &key, const ValueType &val, const KeyComparator &comp) -> bool;

  // 将当前已经满的节点拆分成other中去
  auto Split(B_PLUS_TREE_LEAF_PAGE_TYPE *other, const KeyComparator &comp) -> KeyType;

  // 根据传入的Key找到相应的值
  auto FindValueAddVector(const KeyType &key, std::vector<ValueType> *result, const KeyComparator &comp) -> bool;

  // 根据传入的key对当前的叶子节点进行删除,如果删除的是第一个pair那么需要return true,and return second KeyType
  auto DeleteKey(const KeyType &key, const KeyComparator &comp) -> std::pair<bool, KeyType>;

  // 根据传入的keys返回当前key的下标,return -1 没有找到
  auto FindIndexKey(const KeyType &key, const KeyComparator &comp) -> int;

 private:
  // 保存下一个页面的索引
  page_id_t next_page_id_;
  // Flexible array member for page data.
  MappingType array_[LEAF_PAGE_SIZE];
};
}  // namespace bustub
