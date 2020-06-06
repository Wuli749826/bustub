//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_manager.cpp
//
// Identification: src/recovery/log_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_manager.h"
#include <thread>
#include <chrono>
#include <condition_variable>


namespace bustub {
/*
 * set enable_logging = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when the log buffer is full or buffer pool
 * manager wants to force flush (it only happens when the flushed page has a
 * larger LSN than persistent LSN)
 */
void LogManager::Flush() {
    //log buffer is full; log_timeout seconds passed; 
    //When the buffer pool is going to evict a dirty page from the LRU replacer.
    std::unique_lock<std::mutex> lk_(flush_buffer_latch_);
    while (enable_logging) {
        //lamda capture variables by value
        cv_.wait_for(lk_, log_timeout, [=]{return full_==true;});
        //non-block if full or timeout

        //flushing

        //after flush
        persistent_lsn_ = 
    }
    
}

void LogManager::RunFlushThread() {
    enable_logging = true;
    std::thread th = std::thread(Flush);
    flush_thread_ = &th;
}

/*
 * Stop and join the flush thread, set enable_logging = false
 */
void LogManager::StopFlushThread() {
    enable_logging = false;
    flush_thread_->join();
}

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
lsn_t LogManager::AppendLogRecord(LogRecord *log_record) {
    std::lock_guard<std::mutex> log_lock(log_buffer_latch_);
    int32_t log_size_ = log_record->GetSize();
    log_record->lsn_ = next_lsn_++;
    if (offset_ + log_size_ > LOG_BUFFER_SIZE) {
        flush_buffer_latch_.lock();
        full_ = true;
        char* tmp = flush_buffer_;
        flush_buffer_ = log_buffer_;
        log_buffer_ = tmp;
        //offset_ = 0; //flush将logbuffer的offset设置为0
        flush_buffer_latch_.unlock();
        cv_.notify_all();
    }
    memcpy(log_buffer_+offset_, log_record, 20);
    int pos = offset_+20;
    // insert: header + rid + insert_tuple_size + insert_tuple
    if (log_record->log_record_type_ == LogRecordType::INSERT) {
        memcpy(log_buffer_ + pos, &(log_record->insert_rid_), sizeof(RID));
        pos += sizeof(RID);
        log_record->insert_tuple_.SerializeTo(log_buffer_ + pos);
    }
    //new page: header + prev_page_id
    else if (log_record->log_record_type_ == LogRecordType::NEWPAGE) {
        memcpy(log_buffer_+ pos, &(log_record->prev_page_id_), sizeof(page_id_t));
    }
    //delete: header + rid + delete_tuple.size + delete_tuple
    else if (log_record->log_record_type_ == LogRecordType::APPLYDELETE || 
                log_record->log_record_type_ == LogRecordType::MARKDELETE ||
            log_record->log_record_type_ == LogRecordType::ROLLBACKDELETE) {
        memcpy(log_buffer_ + pos, &(log_record->delete_rid_), sizeof(RID));
        pos += sizeof(RID);
        log_record->delete_tuple_.SerializeTo(log_buffer_ + pos);
    }
    //update: header + rid + old_tuple_size + old_tuple + new_tuple_size + new_tuple
    else if (log_record->log_record_type_ == LogRecordType::UPDATE) {
        memcpy(log_buffer_+pos, &(log_record->update_rid_), sizeof(RID));
        pos += sizeof(RID);
        log_record->old_tuple_.SerializeTo(log_buffer_ + pos);
        pos += log_record->old_tuple_.GetLength() + sizeof(int32_t);
        log_record->new_tuple_.SerializeTo(log_buffer_ + pos);
    }
    else if (log_record->log_record_type_ == LogRecordType::BEGIN || 
        log_record->log_record_type_ == LogRecordType::COMMIT ||
        log_record->log_record_type_ == LogRecordType::ABORT) {
            ;
        }
    offset_ += log_size_;
    return log_record->GetLSN();
}

}  // namespace bustub
