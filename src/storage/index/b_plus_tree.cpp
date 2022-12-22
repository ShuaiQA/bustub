#include <ios>
#include <memory>
#include <new>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/index/b_plus_tree.h"
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
  auto leaf_data = FindShouldLocalPage(root_page_id_, key);
  return leaf_data->FindValueAddVector(key, result, comparator_);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindShouldLocalPage(page_id_t root, const KeyType &key) -> LeafPage * {
  // 根据传入的key返回当前的key可能应该插入在哪一个叶子节点中
  auto page = buffer_pool_manager_->FetchPage(root);
  auto data = reinterpret_cast<BPlusTreePage *>(page);
  if (data->IsLeafPage()) {
    return reinterpret_cast<LeafPage *>(data);
  }
  auto internal_data = reinterpret_cast<InternalPage *>(data);
  page_id_t next_page = internal_data->GetNextPageId(key, comparator_);
  buffer_pool_manager_->UnpinPage(root, false);
  return FindShouldLocalPage(next_page, key);
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
  // 如果当前的树是空的,建立一个新的tree,更新root page_id,插入数据,否则插入进叶子页面
  if (IsEmpty()) {
    CreateNewLeafPage(&root_page_id_);
    UpdateRootPageId();
  }
  auto data = FindShouldLocalPage(root_page_id_, key);
  auto v = data->Insert(key, value, comparator_);
  if (!v) {  // 当前的key存在
    buffer_pool_manager_->UnpinPage(data->GetPageId(), false);
    return false;
  }
  Print(buffer_pool_manager_);
  DfsSplit(reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(data->GetParentPageId())), data);
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
    LOG_DEBUG("cur not Split");
    return;
  }
  // 如果一直递归到root那么进行更换root
  if (child->GetParentPageId() == INVALID_PAGE_ID) {  // 当前已经递归到根节点了,child是满的
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
    Print(buffer_pool_manager_);
    LOG_DEBUG("change root is %d", root);
    LOG_DEBUG("cur internal max size is %d", data->GetMaxSize());
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
    LOG_DEBUG("cur internal size is %d", parent->GetSize());
  } else {
    CreateNewInternalPage(&other, parent->GetPageId());
    auto other_data = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(other));
    auto child_data = reinterpret_cast<InternalPage *>(child);
    KeyType mid_key = child_data->Split(other_data, comparator_);
    parent->Insert(mid_key, other_data->GetPageId(), comparator_);
    LOG_DEBUG("cur internal size is %d", parent->GetSize());
  }
  buffer_pool_manager_->UnpinPage(other, true);
  buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  LOG_DEBUG("cur Split parent is %d,split is %d,other is %d", parent->GetPageId(), child->GetPageId(), other);
  Print(buffer_pool_manager_);
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
  if (IsEmpty()) {
    return;
  }
  auto leaf_data = FindShouldLocalPage(root_page_id_, key);
  std::pair<bool, KeyType> cur = leaf_data->DeleteKey(key, comparator_);
  if (cur.first) {
    DfsChangePos0(leaf_data->GetParentPageId(), key, cur.second);
  }
  // 需要删除当前的keys
  // 如果当前的key不是第一个,并且删除之后的size没有小于最小值,直接进行删除
  // 删除之后的大小如果是小于最小值的话,那么需要从next_page中偷一个如果next_page的大小大于min_size
  // 还需要进行修改根节点,
  // 分析如果的当前的key是第一个那么需要进行修改上面的节点,一直到上面的节点不再是第一个为止
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DfsChangePos0(page_id_t father, const KeyType &oldkey, const KeyType &newkey) {
  auto page_data = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(father));
  if (page_data->ChangePos0Key(oldkey, newkey, comparator_)) {
    DfsChangePos0(page_data->GetParentPageId(), oldkey, newkey);
  }
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

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
