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

#include "common/config.h"
#include "common/exception.h"
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
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  next_page_id_ = INVALID_PAGE_ID;
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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &val, const KeyComparator &comp) -> bool {
  // 如果当前key存在返回0,当前的容量满了返回-1,插入成功返回1
  // 因为我们不能够判断叶子节点的pair满的情况下是单数
  // 所以我们不能直接拆分然后添加进去,我们想先添加进去然后在递归的进行拆分
  for (int i = GetSize() - 1; i >= 0; i--) {
    if (comp(array_[i].first, key) == 0) {
      return false;
    }
  }
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
  for (int size = GetMinSize() + 1; size <= GetMaxSize(); size++) {
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

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
