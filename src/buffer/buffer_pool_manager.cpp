//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <list>
#include <unordered_map>
#include "buffer/clock_replacer.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}


//selectframe: select the frame id and flush that page.
//selectframe is called by Fetchpage and Newpage.
bool BufferPoolManager::SelectFrameImpl(frame_id_t &free_frame_id) {
  if (free_list_.size() > 0) {
    free_frame_id = free_list_.front();
    // if (pages_[free_frame_id].page_id_ != INVALID_PAGE_ID && page_table_.find(pages_[free_frame_id].page_id_) != page_table_.end())
    //     page_table_.erase(pages_[free_frame_id].page_id_);
    free_list_.pop_front();
  } else {
    // has no victim, return false
    if (!replacer_->Victim(&free_frame_id)) {
      return false;
    } else {
        //when victim is dirty, we need write it back to disk
        if (pages_[free_frame_id].IsDirty())
            FlushPageImpl(pages_[free_frame_id].page_id_);
    }
  }
  return true;
}

// 1.     Search the page table for the requested page (P).
// 1.1    If P exists, pin it and return it immediately.
// 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
//        Note that pages are always found from the free list first.
// 2.     If R is dirty, write it back to the disk.
// 3.     Delete R from the page table and insert P.
// 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  frame_id_t free_frame_id;
  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_.find(page_id) != page_table_.end()) {
    free_frame_id = page_table_[page_id];
    pages_[free_frame_id].pin_count_++;
    replacer_->Pin(free_frame_id);
    return &(pages_[free_frame_id]);
  }
  if (SelectFrameImpl(free_frame_id) == false) {
    return nullptr;
  }
  char buf[PAGE_SIZE];
  disk_manager_->ReadPage(page_id, buf);
  memcpy(pages_[free_frame_id].data_, buf, PAGE_SIZE);
  pages_[free_frame_id].pin_count_ = 1;
  pages_[free_frame_id].page_id_ = page_id;
  
  page_table_.erase(pages_[free_frame_id].page_id_);
  page_table_[page_id] = free_frame_id;
  return &(pages_[free_frame_id]);
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  // why we need is_dirty
  //latch_.lock();
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id = page_table_[page_id];
  Page *tar = &(pages_[frame_id]);
  //tar->WLatch();
  //latch_.unlock();

  if (tar->GetPinCount() <= 0) {
    //tar->WUnlatch();
    return false;
  } else {
    tar->pin_count_--;
    if (is_dirty) {
      disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    }
    if (tar->pin_count_ == 0)
        replacer_->Unpin(frame_id);
  }
  //tar->WUnlatch();
  return true;
}


//before writing to disk, flush all pages whose lsn < pagelsn to disk
bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    Page *re = &(pages_[frame_id]);
    lsn_t has_done = log_manager_->GetPersistentLSN();
    lsn_t current = pages_[frame_id].GetLSN();
    if (current <= has_done)
        return true;
    //flush all pages between (has done, current)
    
    if (re->is_dirty_) {
      disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
      re->is_dirty_ = false;
    }
    return true;
  }
  return false;
}

// 0.   Make sure you call DiskManager::AllocatePage!
// 1.   If all the pages in the buffer pool are pinned, return nullptr.
// 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
// 3.   Update P's metadata, zero out memory and add P to the page table.
// 4.   Set the page ID output parameter. Return a pointer to P.

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  std::lock_guard<std::mutex> lock(latch_);
  bool no_position = true;
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].pin_count_ == 0) {
      no_position = false;
      break;
    }
  }
  if (no_position) {
    return nullptr;
  }
  *page_id = disk_manager_->AllocatePage();
  // pick a victim, and there must be at least one victim 
  frame_id_t free_frame_id;
  SelectFrameImpl(free_frame_id);
  // project 4: if there is page_id in page_table, don't erase, just overwrite
  //page_table_.erase(pages_[free_frame_id].page_id_);
  page_table_[*page_id] = free_frame_id;
  pages_[free_frame_id].ResetMemory();
  pages_[free_frame_id].page_id_ = *page_id;
  pages_[free_frame_id].pin_count_ = 1;
  return &(pages_[free_frame_id]);
}

// 0.   Make sure you call DiskManager::DeallocatePage!
// 1.   Search the page table for the requested page (P).
// 1.   If P does not exist, return true.
// 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
// 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
   std::lock_guard<std::mutex> lock(latch_);
  disk_manager_->DeallocatePage(page_id);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].GetPinCount() != 0) {
    return false;
  }
  page_table_.erase(page_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  free_list_.push_back(frame_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  // we should lock here, make sure that every page will not change
  // or we copy the page_id, data_ of all pages out, reduce the length of lock
  // latch_.lock();
  std::lock_guard<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    // acquire the read lock, then other threads can read concurrantly
    // pages_[i].RLatch();
    if (pages_[i].IsDirty()) {
        disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
        pages_[i].is_dirty_ = false;
    }
    // pages_[i].RUnlatch();
  }
  // latch_.unlock();
}

}  // namespace bustub
