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

#include <cassert>
#include <iostream>
#include <sstream>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
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
  assert(GetSize() <= GetMaxSize());
  std::cout << std::endl;
  int i = GetSize() - 1;
  while (i >= 1 && comp(array_[i].first, key) > 0) {
    i--;
  }
  return ValueAt(i);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetIndexKeyValue(int index, const KeyType &key, const ValueType &val) {
  array_[index] = {key, val};
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &val, const KeyComparator &comp) {
  // 对于叶子节点的插入不需要进行判断是否重复的
  int i = GetSize() - 1;
  while (i >= 1 && comp(array_[i].first, key) > 0) {
    array_[i + 1] = array_[i];
    i--;
  }
  array_[i + 1] = {key, val};
  IncreaseSize(1);
  assert(GetSize() <= GetMaxSize() + 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::AddKeyTo1ValTo0(const KeyType &key, const ValueType &val) {
  // 添加一个key到位置1和val到位置0,其余的位置依次向后移动
  // 右移动key
  for (int i = GetSize() - 1; i >= 1; i--) {
    array_[i + 1] = array_[i];
  }
  array_[1].second = array_[0].second;
  SetKeyAt(1, key);
  array_[0].second = val;
  IncreaseSize(1);
  assert(GetSize() <= GetMaxSize());
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteKey1Val0() -> MappingType {
  // 删除key的位置1和val的位置0返回
  KeyType key = array_[1].first;
  ValueType val = array_[0].second;
  array_[0].second = array_[1].second;
  for (int i = 1; i < GetSize(); i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  return MappingType{key, val};
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(B_PLUS_TREE_INTERNAL_PAGE_TYPE *other, const KeyComparator &comp,
                                           BufferPoolManager *buffer) -> KeyType {
  // 插入最初的第一个{}
  other->Insert(KeyType{}, array_[GetMinSize() + 1].second, comp);
  auto in = reinterpret_cast<InternalPage *>(buffer->FetchPage(array_[GetMinSize() + 1].second));
  in->SetParentPageId(other->GetPageId());
  buffer->UnpinPage(array_[GetMinSize() + 1].second, true);
  for (int size = GetMinSize() + 2; size <= GetMaxSize(); size++) {
    other->Insert(array_[size].first, array_[size].second, comp);
    auto internal = reinterpret_cast<InternalPage *>(buffer->FetchPage(array_[size].second));
    internal->SetParentPageId(other->GetPageId());
    buffer->UnpinPage(array_[size].second, true);
  }
  IncreaseSize(GetMinSize() - GetMaxSize());
  return array_[GetMinSize() + 1].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ChangePos0Key(const KeyType &oldkey, const KeyType &newkey,
                                                   const KeyComparator &comp) -> bool {
  bool ans = false;
  for (int i = GetSize() - 1; i > 0; i--) {
    if (comp(array_[i].first, oldkey) == 0) {
      SetKeyAt(i, newkey);
      ans = true;
      break;
    }
  }
  return ans;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::AccordValFindValPos(const ValueType &val) -> int {
  int i = GetSize() - 1;
  while (i != -1) {
    if (array_[i].second == val) {
      break;
    }
    i--;
  }
  return i;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
