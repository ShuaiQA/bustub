//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <sstream>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 初始化相关的值,设置size==1,type==leaf
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size, page_id_t next_page_id) {
  SetNextPageId(next_page_id);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetKeyAndValue(int index) const -> const MappingType & { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindIndexKey(const KeyType &key, const KeyComparator &comp) -> int {
  int i = GetSize() - 1;
  while (i > 0 && comp(array_[i].first, key) == 0) {
    i--;
  }
  return i;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &val, const KeyComparator &comp) -> bool {
  // 当前的key存在返回false
  for (int i = GetSize() - 1; i >= 0; i--) {
    if (comp(array_[i].first, key) == 0) {
      return false;
    }
  }
  // 逆向插入的位置
  int i = GetSize() - 1;
  while (i >= 0 && comp(array_[i].first, key) > 0) {
    array_[i + 1] = array_[i];
    i--;
  }
  array_[i + 1] = {key, val};
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Split(B_PLUS_TREE_LEAF_PAGE_TYPE *other, const KeyComparator &comp) -> KeyType {
  for (int size = GetMinSize(); size < GetMaxSize(); size++) {
    other->Insert(array_[size].first, array_[size].second, comp);
  }
  IncreaseSize(GetMinSize() - GetMaxSize());
  next_page_id_ = other->GetPageId();
  return other->array_[0].first;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindValueAddVector(const KeyType &key, std::vector<ValueType> *result,
                                                    const KeyComparator &comp) -> bool {
  bool ans = false;
  for (int i = 0; i < GetSize(); i++) {
    if (comp(array_[i].first, key) == 0) {
      ans = true;
      result->push_back(array_[i].second);
    }
  }
  return ans;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteKey(const KeyType &key, const KeyComparator &comp) -> std::pair<bool, KeyType> {
  // we need pos == 0 and combine?
  // 删除key,如果删除的是数组的下标0,返回true,并且返回删除之后的第0个下标
  int i = 0;
  bool ans = false;
  while (comp(array_[i].first, key) != 0 && i < GetSize()) {
    i++;
  }
  if (i == 0) {
    ans = true;
  }
  if (i != GetSize()) {
    while (i < GetSize()) {
      array_[i] = array_[i + 1];
      i++;
    }
    IncreaseSize(-1);
  }
  return std::pair<bool, KeyType>{ans, array_[0].first};
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
