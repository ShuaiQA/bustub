#include <ios>
#include <iostream>
#include <memory>
#include <new>
#include <sstream>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"
#include "storage/page/page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 * 如果当前的页面是无效的代表当前的节点是空的
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  auto leaf_data = FindShouldLocalPage(root_page_id_, key, transaction);
  return leaf_data->FindValueAddVector(key, result, comparator_);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindShouldLocalPage(page_id_t root, const KeyType &key, Transaction *transaction) -> LeafPage * {
  // 根据传入的key返回当前的key可能应该插入在哪一个叶子节点中
  Page *page = buffer_pool_manager_->FetchPage(root);
  auto data = reinterpret_cast<BPlusTreePage *>(page);
  if (data->IsLeafPage()) {
    return reinterpret_cast<LeafPage *>(data);
  }
  auto internal_data = reinterpret_cast<InternalPage *>(data);
  page_id_t next_page = internal_data->GetNextPageId(key, comparator_);
  buffer_pool_manager_->UnpinPage(root, false);
  return FindShouldLocalPage(next_page, key, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateNewLeafPage(page_id_t *page_id, page_id_t parent) -> bool {
  auto page = buffer_pool_manager_->NewPage(page_id);
  auto data = reinterpret_cast<LeafPage *>(page);
  // 对于根节点进行set函数,设置大小、父页面、当前页面、当前类别
  data->Init(*page_id, parent, leaf_max_size_);
  buffer_pool_manager_->UnpinPage(*page_id, true);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateNewInternalPage(page_id_t *page_id, page_id_t parent) -> bool {
  auto page = buffer_pool_manager_->NewPage(page_id);
  auto data = reinterpret_cast<InternalPage *>(page);
  // 对于根节点进行set函数,设置大小、父页面、当前页面、当前类别
  data->Init(*page_id, parent, internal_max_size_);
  buffer_pool_manager_->UnpinPage(*page_id, true);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  LOG_INFO("insert");
  // 如果当前的树是空的,建立一个新的tree,更新root page_id,插入数据,否则插入进叶子页面
  if (IsEmpty()) {
    CreateNewLeafPage(&root_page_id_);
    UpdateRootPageId();
  }
  auto data = FindShouldLocalPage(root_page_id_, key, transaction);
  auto v = data->Insert(key, value, comparator_);
  if (!v) {  // 当前的key存在
    buffer_pool_manager_->UnpinPage(data->GetPageId(), false);
    return false;
  }
  DfsSplit(reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(data->GetParentPageId())), data);
  Print(buffer_pool_manager_);
  return true;
}
/**
 * 根据当前的函数我们需要不断的对于当前的child进行拆分然后向上提取某个关键字
 * 直到最后我们有可能会发现root作为child已经满了
 * 这个时候我们需要重新设置一个根节点,让其孩子节点指向child。
 * 我们首先找到递归出口,也就是说我们的child应该是大于size的才会执行当前的函数
 * 否则当前的child没有超出大小不需要进行拆分
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DfsSplit(InternalPage *parent, BPlusTreePage *child) {
  if (!child->IsFull()) {  // 如果当前的孩子没有满直接返回
    return;
  }
  LOG_INFO("Split");
  // 如果一直递归到root那么进行更换root
  if (child->GetParentPageId() == INVALID_PAGE_ID) {  // 当前已经递归到根节点了,child是满的
    LOG_INFO("当前需要更新root");
    // 根据递归我们会发现child页面已经满了,但是parent是无效的
    page_id_t root;
    CreateNewInternalPage(&root);  // 创建一个新的root当作新节点
    auto data = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(root));
    data->SetIndexKeyValue(0, KeyType{}, child->GetPageId());
    data->IncreaseSize(1);
    parent = data;
    root_page_id_ = root;  // 修改当前的root_page_id_
    child->SetParentPageId(root);
    UpdateRootPageId();
  }
  // 如果当前的孩子满了需要进行拆分,由于上面的根节点的设置,我们始终可以保证移动到上面的是有节点可以插入的
  // 需要对当前child节点进行拆分,然后递归执行parent节点
  page_id_t other;
  if (child->IsLeafPage()) {
    CreateNewLeafPage(&other, parent->GetPageId());
    auto other_data = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(other));
    auto child_data = reinterpret_cast<LeafPage *>(child);
    KeyType mid_key = child_data->Split(other_data, comparator_);
    parent->Insert(mid_key, other_data->GetPageId(), comparator_);
  } else {
    CreateNewInternalPage(&other, parent->GetPageId());
    auto other_data = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(other));
    auto child_data = reinterpret_cast<InternalPage *>(child);
    KeyType mid_key = child_data->Split(other_data, comparator_);
    parent->Insert(mid_key, other_data->GetPageId(), comparator_);
  }
  buffer_pool_manager_->UnpinPage(other, true);
  buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  DfsSplit(reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent->GetParentPageId())), parent);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  LOG_INFO("remove");
  if (IsEmpty()) {
    return;
  }
  auto leaf_data = FindShouldLocalPage(root_page_id_, key, transaction);
  std::pair<bool, KeyType> cur = leaf_data->DeleteKey(key, comparator_);
  if (leaf_data->IsRootPage()) {  // 如果当前节点是根节点直接删除
    buffer_pool_manager_->UnpinPage(leaf_data->GetPageId(), true);
    return;
  }
  if (leaf_data->GetSize() >= leaf_data->GetMinSize()) {  // 删除之后的大小依旧是没有问题的
    if (cur.first) {                                      // 查看删除的是不是第一个
      DfsChangePos0(leaf_data->GetParentPageId(), key, cur.second);
    }
    buffer_pool_manager_->UnpinPage(leaf_data->GetPageId(), true);
  } else {  // 删除之后的情况,也就是需要查找相关的左右节点进行借取或者合并
    // 借左兄弟的节点
    auto leaf_leaf_data = FindLeafLeafData(leaf_data);
    if (leaf_leaf_data && leaf_leaf_data->GetSize() > leaf_leaf_data->GetMinSize() &&
        leaf_data->GetParentPageId() == leaf_leaf_data->GetParentPageId()) {
      // 获取左兄弟的第一个节点的最后一个值,获取当前节点的第一个数组值(为了修改父节点),删除左面最后一个值,将该值插入到右面的节点中
      MappingType leaf_right = leaf_leaf_data->GetKeyAndValue(leaf_leaf_data->GetSize() - 1);
      // 需要根据当前的page_id来获取父节点的key,然后修改修改父节点的key
      auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(leaf_data->GetParentPageId()));
      auto olderkey = parent->KeyAt(parent->AccordValFindValPos(leaf_data->GetPageId()));
      buffer_pool_manager_->UnpinPage(leaf_data->GetParentPageId(), false);
      leaf_leaf_data->DeleteKey(leaf_right.first, comparator_);
      leaf_data->Insert(leaf_right.first, leaf_right.second, comparator_);
      // 因为获取的是左边的节点,必定放到当前节点的第一个位置,需要进行修改父节点的第一个位置
      DfsChangePos0(leaf_data->GetParentPageId(), olderkey, leaf_right.first);
      return;
    }
    // 借右兄弟的节点
    auto right_data = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(leaf_data->GetNextPageId()));
    if (right_data && right_data->GetSize() > right_data->GetMinSize() &&
        right_data->GetParentPageId() == leaf_data->GetParentPageId()) {
      // 获取右节点的第一个进行放到左节点的右边,修改右边节点的第一个为第二个索引
      // 首先需要删除右节点的第一个节点,并返回删除之后的下标0节点用于修改父节点的索引
      // 之后将当前获取的节点放到当前的节点上面,查看是否需要修改当前的节点的父节点(当前的min==1,删除之后是0的情况)
      MappingType pos0 = right_data->GetKeyAndValue(0);
      auto begin = Begin(key);
      ++begin;          // begin 获取当前节点的后继节点
      if (cur.first) {  // 如果删除的是第一个节点那么需要将后继节点和当前key进行修改
        MappingType cur = *begin;
        DfsChangePos0(right_data->GetParentPageId(), key, cur.first);
      }
      std::pair<bool, KeyType> right_delete = right_data->DeleteKey(right_data->KeyAt(0), comparator_);
      // 修改右面节点的索引
      DfsChangePos0(right_data->GetParentPageId(), pos0.first, right_delete.second);
      leaf_data->Insert(pos0.first, pos0.second, comparator_);
      return;
    }
    // 将当前节点合并到左面的节点,删除当前节点的父节点的索引,修改前面的节点的next指向当前节点的next的索引
    if (leaf_leaf_data && leaf_leaf_data->GetParentPageId() == leaf_data->GetParentPageId()) {
      // 当前节点合并到左边的节点
      while (leaf_leaf_data->GetSize() != 0) {
        auto arr = leaf_data->GetKeyAndValue(0);
        leaf_leaf_data->Insert(arr.first, arr.second, comparator_);
        leaf_data->DeleteKey(arr.first, comparator_);
      }
      // 修改前面的节点的next执行当前节点的next
      leaf_leaf_data->SetNextPageId(leaf_data->GetNextPageId());
      // 此处应该删除父节点一个关键字删除的是page_id = leaf_data->GetPageId()，继续向上递归的进行
      auto father_data =
          reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(leaf_data->GetParentPageId()));
      father_data->DeleteArrayVal(leaf_data->GetPageId());
      buffer_pool_manager_->UnpinPage(leaf_data->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(father_data->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(leaf_leaf_data->GetPageId(), true);
      if (right_data != nullptr) {
        buffer_pool_manager_->UnpinPage(right_data->GetPageId(), false);
      }
      // 需要判断当前的父节点是不是根节点,如果是根节点范围是最低2,如果是1的话,将孩子节点提取到根节点
      DfsShouldCombine(father_data);
      return;
    }
    // 右边节点和并到当前节点
    if (right_data && right_data->GetParentPageId() == leaf_data->GetParentPageId()) {
      // 如果删除的是第一个节点,获取当前节点的直接后继节点进行修改父节点的索引
      if (cur.first) {
        auto begin = Begin(key);
        ++begin;
        MappingType data = *begin;
        DfsChangePos0(right_data->GetParentPageId(), key, data.first);
      }
      // 将右面节点的值合并到当前节点
      while (right_data->GetSize() != 0) {
        auto arr = right_data->GetKeyAndValue(0);
        leaf_data->Insert(arr.first, arr.second, comparator_);
        right_data->DeleteKey(arr.first, comparator_);
      }
      // 修改当前节点的next指向右面节点的next
      leaf_data->SetNextPageId(right_data->GetNextPageId());
      // 此处应该删除父节点一个关键字，根据右面节点的page_id 进行向上面查找value的值，继续向上递归的进行
      auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(right_data->GetParentPageId()));
      // 右边节点删除了一个值,需要递归的修改父节点的值
      parent->DeleteArrayVal(right_data->GetPageId());
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(right_data->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(leaf_data->GetPageId(), true);
      DfsShouldCombine(parent);
      return;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DfsShouldCombine(InternalPage *cur) {
  // 如果当前的节点是根节点需要进行操作
  if (cur->IsRootPage()) {
    if (cur->GetSize() < 2) {  // 当前的根节点只有一个节点也就是0,修改根节点
      root_page_id_ = cur->ValueAt(0);
      UpdateRootPageId();
    }
    buffer_pool_manager_->UnpinPage(cur->GetPageId(), false);
    return;
  }
  if (cur->GetSize() >= cur->GetMinSize()) {  // 不是根节点那么需要严格的比较minsize
    buffer_pool_manager_->UnpinPage(cur->GetPageId(), false);
    return;
  }
  // 依旧是需要进行左右节点的借取或者是左右节点的合并
  auto leaf = FindInternalLeafData(cur);
  // 当前节点向左边的内部节点借
  if (leaf && leaf->GetSize() > leaf->GetMinSize()) {
    auto father = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(leaf->GetParentPageId()));
    // 获取左边节点的最后一个节点需要将其放到右面
    KeyType key = leaf->KeyAt(leaf->GetSize() - 1);
    page_id_t val = leaf->ValueAt(leaf->GetSize() - 1);
    leaf->DeleteArrayVal(val);
    // 当前节点对应于的父节点的index,需要获得父节点的key放到cur中,将左边节点的key放到父节点中,将左边节点的val放到cur节点中
    int index = father->AccordValFindValPos(leaf->GetPageId());
    KeyType father_key = father->KeyAt(index);
    father->SetKeyAt(index, key);
    cur->Insert(father_key, val, comparator_);
    buffer_pool_manager_->UnpinPage(leaf->GetParentPageId(), true);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(cur->GetPageId(), true);
    return;
  }
  auto right = FindInternalRightData(cur);
  // 当前节点向右边的内部节点借
  if (right && right->GetSize() > right->GetMinSize()) {
    auto father = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(cur->GetParentPageId()));
    // 获取右边节点的第一个节点需要将其放到当前,并删除右边节点
    KeyType key = right->KeyAt(0);
    page_id_t val = right->ValueAt(0);
    right->DeleteArrayVal(val);
    // 当前节点对应于的父节点的index+1(注意是index+1),需要获得父节点的key放到cur中
    // 将左边节点的key放到父节点中,将左边节点的val放到cur节点中
    int index = father->AccordValFindValPos(right->GetPageId()) + 1;
    KeyType father_key = father->KeyAt(index);
    father->SetKeyAt(index, key);
    cur->Insert(father_key, val, comparator_);
    buffer_pool_manager_->UnpinPage(right->GetParentPageId(), true);
    buffer_pool_manager_->UnpinPage(right->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(cur->GetPageId(), true);
    if (leaf != nullptr) {
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    }
    return;
  }
  // 向左边的内部节点进行合并
  if (leaf) {
    // cur节点的父节点拉下来放到左边节点,将当前节点的所有的数据合并到左边
    auto father = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(leaf->GetParentPageId()));
    int index = father->AccordValFindValPos(cur->GetPageId());
    KeyType father_key = father->KeyAt(index);  // key是父节点的key,value是右边的第一个
    // 父亲节点删除对应的key
    father->DeleteArrayVal(cur->GetPageId());
    // 将内部节点的cur所有的数据,放到leaf中,其中第一个key{} 应该改为key变量
    for (int i = 0; i < cur->GetSize(); i++) {
      KeyType key = cur->KeyAt(0);
      page_id_t val = cur->ValueAt(0);
      if (i == 0) {
        key = father_key;
      }
      cur->DeleteArrayVal(val);
      leaf->Insert(key, val, comparator_);
    }
    buffer_pool_manager_->UnpinPage(leaf->GetParentPageId(), true);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(cur->GetPageId(), true);
    DfsShouldCombine(father);
    return;
  }
  // 右面节点向cur进行合并
  if (right) {
    // cur节点的父节点拉下来放到左边节点,将当前节点的所有的数据合并到左边
    auto father = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(right->GetParentPageId()));
    int index = father->AccordValFindValPos(cur->GetPageId()) + 1;
    KeyType father_key = father->KeyAt(index);  // key是父节点的key,value是右边的第一个
    // 父亲节点删除对应的key
    father->DeleteArrayVal(right->GetPageId());
    // 将内部节点的cur所有的数据,放到leaf中,其中第一个key{} 应该改为key变量
    for (int i = 0; i < right->GetSize(); i++) {
      KeyType key = right->KeyAt(0);
      page_id_t val = right->ValueAt(0);
      if (i == 0) {
        key = father_key;
      }
      right->DeleteArrayVal(val);
      cur->Insert(key, val, comparator_);
    }
    buffer_pool_manager_->UnpinPage(right->GetParentPageId(), true);
    buffer_pool_manager_->UnpinPage(cur->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right->GetPageId(), true);
    if (leaf != nullptr) {
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    }
    DfsShouldCombine(father);
    return;
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindInternalLeafData(InternalPage *cur) -> InternalPage * {
  if (cur->IsRootPage()) {
    return nullptr;
  }
  auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(cur->GetParentPageId()));
  int index = parent->AccordValFindValPos(cur->GetPageId());
  if (index <= 0) {
    return nullptr;
  }
  page_id_t pre = parent->ValueAt(index - 1);
  buffer_pool_manager_->UnpinPage(cur->GetParentPageId(), false);
  return reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(pre));
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindInternalRightData(InternalPage *cur) -> InternalPage * {
  if (cur->IsRootPage()) {
    return nullptr;
  }
  auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(cur->GetParentPageId()));
  int index = parent->AccordValFindValPos(cur->GetPageId());
  if (index >= parent->GetSize() - 1) {
    return nullptr;
  }
  page_id_t pre = parent->ValueAt(index + 1);
  buffer_pool_manager_->UnpinPage(cur->GetParentPageId(), false);
  return reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(pre));
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafLeafData(LeafPage *cur) -> LeafPage * {
  if (cur->IsRootPage()) {  // 如果当前的cur是根节点,根节点没有兄弟
    return nullptr;
  }
  auto leaf = reinterpret_cast<LeafPage *>(cur);
  auto internal = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(leaf->GetParentPageId()));
  int index = internal->AccordValFindValPos(leaf->GetPageId());
  if (index <= 0) {
    return nullptr;
  }
  return reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(internal->ValueAt(index - 1)));
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DfsChangePos0(page_id_t father, const KeyType &oldkey, const KeyType &newkey) {
  // 递归到出口直接返回
  if (father == INVALID_PAGE_ID) {
    return;
  }
  auto page_data = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(father));
  // 查看在当前的根节点是否找到对应的目标值
  if (page_data->ChangePos0Key(oldkey, newkey, comparator_)) {  // 如果找到了可以不用递归了
    buffer_pool_manager_->UnpinPage(father, true);
    return;
  }
  // 没有找到,不用修改当前的页面
  buffer_pool_manager_->UnpinPage(father, false);
  DfsChangePos0(page_data->GetParentPageId(), oldkey, newkey);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  LeafPage *leaf = GetFirstLeafData(root_page_id_);
  return INDEXITERATOR_TYPE(leaf, 0, buffer_pool_manager_);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetFirstLeafData(page_id_t root) -> LeafPage * {
  // 获取第一个叶子节点
  auto data = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root));
  if (data->IsLeafPage()) {
    return reinterpret_cast<LeafPage *>(data);
  }
  auto internal = reinterpret_cast<InternalPage *>(data);
  page_id_t temp = internal->ValueAt(0);
  buffer_pool_manager_->UnpinPage(root, false);
  return GetFirstLeafData(temp);
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLastLeafData(page_id_t root) -> LeafPage * {
  // 获取第一个叶子节点
  auto data = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root));
  if (data->IsLeafPage()) {
    return reinterpret_cast<LeafPage *>(data);
  }
  auto internal = reinterpret_cast<InternalPage *>(data);
  page_id_t temp = internal->ValueAt(internal->GetSize() - 1);
  buffer_pool_manager_->UnpinPage(root, false);
  return GetLastLeafData(temp);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto leaf = FindShouldLocalPage(root_page_id_, key);
  auto index = leaf->FindIndexKey(key, comparator_);
  return INDEXITERATOR_TYPE(leaf, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  auto last = GetLastLeafData(root_page_id_);
  return INDEXITERATOR_TYPE(last, last->GetSize(), buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
