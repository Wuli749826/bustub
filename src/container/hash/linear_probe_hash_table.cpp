//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"
#include "storage/page/hash_table_block_page.h"
#include "storage/page/hash_table_header_page.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
        table_size_ = num_buckets;
        head_page = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(&header_page_id_, nullptr)->GetData());
        page_id_t block_page_id_ = INVALID_PAGE_ID;
        auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>
                (buffer_pool_manager_->NewPage(&block_page_id_, nullptr)->GetData());
        num_ = block_page->Size();

        size_t num_block_page = table_size_/num_ + (table_size_ % num_ > 0);
        while (num_block_page--){
            head_page->AddBlockPageId(block_page_id_);  // head_page->block[0] = block_page
            block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>
                (buffer_pool_manager_->NewPage(&block_page_id_, nullptr)->GetData());
        }
    }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
   * Performs a point query on the hash table.
   * @param transaction the current transaction
   * @param key the key to look up
   * @param[out] result the value(s) associated with a given key
   * @return the value(s) associated with the given key
   */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
    std::cout << "get  " <<  key << std::endl;
    auto bucket_id = hash_fn_.GetHash(key) % table_size_;
    size_t cnt = 0, num_page = bucket_id / num_, offset = bucket_id % num_;
    auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>
            (buffer_pool_manager_->FetchPage(head_page->GetBlockPageId(num_page), nullptr));
    while (cnt < table_size_){
        while (offset < num_ && cnt < table_size_){
            if (block_page->IsOccupied(offset) && block_page->IsReadable(offset) && 
                comparator_(block_page->KeyAt(offset),key) == 0)
                result->push_back(block_page->ValueAt(offset));
            offset++;
            cnt++;
        }
        offset = 0;
        num_page = (num_page+1) % (head_page->NumBlocks());
        block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>
            (buffer_pool_manager_->FetchPage(head_page->GetBlockPageId(num_page), nullptr));
    }
    return result->size() > 0;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
   * Inserts a key-value pair into the hash table.
   * @param transaction the current transaction
   * @param key the key to create
   * @param value the value to be associated with the key
   * @return true if insert succeeded, false otherwise
   */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
    std::cout << "insert   " << key << std::endl;
    auto bucket_id = hash_fn_.GetHash(key) % table_size_;
    size_t cnt = 0, num_page = bucket_id / num_, offset = bucket_id % num_;
    HashTableBlockPage<KeyType, ValueType, KeyComparator> * block_page = 
        reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>
            (buffer_pool_manager_->FetchPage(head_page->GetBlockPageId(num_page), nullptr));
    size_t ans_offset, ans_num_page;
    bool find_ans = false;
    while (cnt < table_size_){
        while (offset < num_ && cnt < table_size_){
            if (block_page->IsOccupied(offset)){
                if (block_page->IsReadable(offset) && comparator_(block_page->KeyAt(offset), key)==0 
                     && block_page->ValueAt(offset) == value)
                     return false;
            }
            else if (!find_ans){
                find_ans = true;
                ans_offset = offset;
                ans_num_page = num_page;
            }
            offset++;
            cnt++;

        }
        offset = 0;
        num_page = (num_page+1) % head_page->NumBlocks();
        block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>
            (buffer_pool_manager_->FetchPage(head_page->GetBlockPageId(num_page), nullptr));
    }
    if (!find_ans)
        return false;
    block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>
            (buffer_pool_manager_->FetchPage(head_page->GetBlockPageId(ans_num_page), nullptr));
    block_page->Insert(ans_offset, key, value);
    return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
  /**
   * Deletes the associated value for the given key.
   * @param transaction the current transaction
   * @param key the key to delete
   * @param value the value to delete
   * @return true if remove succeeded, false otherwise
   */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
    std::cout << "remove   " << key << std::endl;
    auto bucket_id = hash_fn_.GetHash(key) % table_size_;
    size_t cnt = 0, num_page = bucket_id / num_, offset = bucket_id % num_;
    HashTableBlockPage<KeyType, ValueType, KeyComparator> * block_page = 
        reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>
            (buffer_pool_manager_->FetchPage(head_page->GetBlockPageId(num_page), nullptr));
    while (cnt < table_size_){
        while (offset < num_ && cnt < table_size_){
            if (block_page->IsOccupied(offset) && block_page->IsReadable(offset) && comparator_(block_page->KeyAt(offset), key)==0 && block_page->ValueAt(offset) == value){
                block_page->Remove(offset);
                return true;
            }
            offset++;
            cnt++;
        }
        offset = 0;
        num_page = (num_page+1) % head_page->NumBlocks();
        block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>
            (buffer_pool_manager_->FetchPage(head_page->GetBlockPageId(num_page), nullptr));
    }
    return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
/**
   * Resizes the table to at least twice the initial size provided.
   * @param initial_size the initial size of the hash table
   */

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
    std::cout << "resize   " << std::endl;
    table_size_ = initial_size * 2;
    size_t new_num_blocks = table_size_/num_ + (table_size_%num_ > 0) - head_page->NumBlocks();
    while (new_num_blocks--){
        page_id_t block_page_id_ = INVALID_PAGE_ID;
        block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>
            (buffer_pool_manager_->NewPage(&block_page_id_, nullptr)->GetData());
        head_page->AddBlockPageId(block_page_id_);  // head_page->block[0] = block_page
    }
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
/**
   * Gets the size of the hash table
   * @return current size of the hash table
   */
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
    // std::cout << "this is linear getsize" << std::endl;
  return table_size_;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
