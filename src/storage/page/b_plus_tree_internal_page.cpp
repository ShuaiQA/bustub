//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page max_size
 * 设置size==1,type==internal
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
  SetSize(0);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteArrayVal(const ValueType &val) -> MappingType {
  // 根据传入的val的值进行查找是否存在当前相关的val进行删除操作
  int i = 0;
  MappingType ans;
  while (array_[i].second != val) {
    i++;
  }
  if (i != GetSize()) {
    ans = array_[i];
  }
  while (i < GetSize()) {
    array_[i] = array_[i + 1];
    i++;
  }
  IncreaseSize(-1);
  return ans;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetNextPageId(const KeyType &key, const KeyComparator &comp) -> page_id_t {
  // 根据当前的key进行在内部节点中进行查找,获取到ValueType也就是下一个页面的值
  int i = GetSize() - 1;
  while (i >= 1 && comp(array_[i].first, key) > 0) {
    i--;
  }
  return array_[i].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetIndexKeyValue(int index, const KeyType &key, const ValueType &val) {
  array_[index] = {key, val};
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &val, const KeyComparator &comp) {
  // 对于叶子节点的插入不需要进行判断是否重复的
  int i = GetSize() - 1;
  while (i >= 0 && comp(array_[i].first, key) > 0) {
    array_[i + 1] = array_[i];
    i--;
  }
  array_[i + 1] = {key, val};
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(B_PLUS_TREE_INTERNAL_PAGE_TYPE *other, const KeyComparator &comp)
    -> KeyType {
  // 插入最初的第一个{}
  other->Insert(KeyType{}, array_[GetMinSize() + 1].second, comp);
  for (int size = GetMinSize() + 2; size <= GetMaxSize(); size++) {
    other->Insert(array_[size].first, array_[size].second, comp);
  }
  IncreaseSize(GetMinSize() - GetMaxSize());
  return array_[GetMinSize() + 1].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ChangePos0Key(const KeyType &oldkey, const KeyType &newkey,
                                                   const KeyComparator &comp) -> bool {
  bool ans = false;
  for (int i = 0; i < GetSize(); i++) {
    if (comp(array_[i].first, oldkey) == 0) {
      SetKeyAt(i, newkey);
      ans = true;
    }
  }
  return ans;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::AccordValFindValPos(const ValueType &val) -> int {
  int ans = -1;
  for (int i = 1; i < GetSize(); i++) {
    if (array_[i].second == val) {
      ans = i;
      break;
    }
  }
  return ans;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
