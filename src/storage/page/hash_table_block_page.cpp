//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_block_page.cpp
//
// Identification: src/storage/page/hash_table_block_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_block_page.h"
#include "storage/index/generic_key.h"
#include <math.h>
#include <iostream>

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BLOCK_TYPE::KeyAt(slot_offset_t bucket_ind) const {
    return array_[bucket_ind].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BLOCK_TYPE::ValueAt(slot_offset_t bucket_ind) const {
    return array_[bucket_ind].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::Insert(slot_offset_t bucket_ind, const KeyType &key, const ValueType &value) {
    
    int i = bucket_ind / 8;
    int j = 7 - (bucket_ind % 8);
    bool occupy = (occupied_[i] >> j) & 1;
    if (occupy) {
        return false;
    }
    MappingType a;
    a.first = key;
    a.second = value;
    a.swap(array_[bucket_ind]);
    /*
    std::cout << "insert : i, j " <<  i << " " << j << std::endl;
    std::cout << "before: " << (int)(occupied_[i]) << "after " <<  (int)(occupied_[i] | (1 << j)) << std::endl;
    */
    occupied_[i] = occupied_[i] | (1 << j);
    readable_[i] = readable_[i] | (1 << j);
    return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::Remove(slot_offset_t bucket_ind) {
    // std::cout << "this is block remove" << std::endl;
    int i = bucket_ind / 8;
    int j = 7 - (bucket_ind % 8);
    /*
    std::cout << "remove : i, j " <<  i << " " << j << std::endl;
    std::cout << "remove to  " <<  (int)(occupied_[i] & (255 - (int)pow(2, j))) << std::endl;
    */
   //注意： remove 不会将occupied 置为0， 只会将readable 置为0
    //occupied_[i] = occupied_[i] & (255 - (int)pow(2, j));
    
    readable_[i] = readable_[i] & (255 - (int)pow(2, j));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsOccupied(slot_offset_t bucket_ind) const {
    int i = bucket_ind / 8;
    int j = 7 - (bucket_ind % 8);
    /*
    std::cout << "occupied: i, j " <<  i << " " << j << std::endl;
    std::cout << "occupied_[i] " << occupied_[i] << std::endl;
    std::cout << "change to : " << ((occupied_[i] >> j) & 1 )<< std::endl;
    */
    bool occupy = (occupied_[i] >> j) & 1;
    return occupy;

}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsReadable(slot_offset_t bucket_ind) const {
    int i = bucket_ind / 8;
    int j = 7 - (bucket_ind % 8);
    bool read = (readable_[i] >> j) & 1;
    if (read)
        return true;
    return false;
}


template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_BLOCK_TYPE::Size() const {
    return (PAGE_SIZE-((BLOCK_ARRAY_SIZE - 1) / 8 + 1)*2)/sizeof(MappingType);
}


// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBlockPage<int, int, IntComparator>;
template class HashTableBlockPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBlockPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBlockPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBlockPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBlockPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
