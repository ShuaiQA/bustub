/**
 * index_iterator.cpp
 */
#include <strings.h>
#include <cassert>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(LeafPage *leaf, int index, BufferPoolManager *buffer_pool_manager)
    : leaf_(leaf), index_(index), buffer_pool_manager_(buffer_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return leaf_->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_->GetSize() - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_->GetKeyAndValue(index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  index_++;
  if (index_ == leaf_->GetSize() && leaf_->GetNextPageId() != INVALID_PAGE_ID) {
    auto next_page = leaf_->GetNextPageId();
    buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
    leaf_ = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(next_page));
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
