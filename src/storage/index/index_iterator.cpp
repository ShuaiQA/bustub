/**
 * index_iterator.cpp
 */
#include <strings.h>
#include <cassert>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page, int index, BufferPoolManager *buffer_pool_manager)
    : page_(page), index_(index), buffer_pool_manager_(buffer_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  auto cur = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_));
  return cur->GetNextPageId() == INVALID_PAGE_ID && index_ == cur->GetSize() - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto cur = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_));
  const MappingType &c = cur->GetKeyAndValue(index_);
  buffer_pool_manager_->UnpinPage(page_, false);
  return c;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  index_++;
  auto leaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_));
  if (index_ == leaf->GetSize() && leaf->GetNextPageId() != INVALID_PAGE_ID) {
    auto next_page = leaf->GetNextPageId();
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    leaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(next_page));
    page_ = next_page;
    buffer_pool_manager_->UnpinPage(next_page, false);
    index_ = 0;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
