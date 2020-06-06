//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"
#include <algorithm>
#include <vector>
#include <iostream>

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
    total_size = num_pages;
}

ClockReplacer::~ClockReplacer() = default;

/*
Starting from the current position of clock hand, 
find the first frame that is both in the `ClockReplacer` and with its ref flag set to false. 
If a frame is in the `ClockReplacer`, but its ref flag is set to true, change it to false instead. 
This should be the only method that updates the clock hand.
*/
bool ClockReplacer::Victim(frame_id_t *frame_id) { 
    // select a victim, and use frame_id return
    std::lock_guard<std::mutex> lock(clock_latch_);
    if (!clock_size) {
        frame_id = nullptr;
        return false;
    }
    //attention: we cant search only for one circle
    //attention: not thread-safe, for the clock_size should be protected by mutex
    while (1){
        if (!clock_ref[clock_hand]){
            *frame_id = clock_frames[clock_hand];
            clock_frames.erase(clock_frames.begin()+clock_hand);
            //clock_hand = (clock_hand+1)%clock_size;
            //should we clock_size-1 here?
            clock_size--;
            return true;
        }
        else
        {
            clock_ref[clock_hand] = false;
            clock_hand = (clock_hand+1)%clock_size;
        }
    }
    return false;
}


/*
This method should be called after a page is pinned to a frame in the BufferPoolManager. 
It should remove the frame containing the pinned page from the ClockReplacer.
*/
//because this frame is in clockplacer, it can be removed, so we should pin it, remove it out from replacer
//then this page in frame will never be removed from bufferpool
void ClockReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(clock_latch_);
    int32_t i;
    bool exit = false;
    for (i = 0; i < clock_size; ++i){
        if (clock_frames[i] == frame_id) {
            exit = true;
            break;
        }
    }
    if (!exit)
        return;
    clock_frames.erase(clock_frames.begin()+i);
    clock_ref.erase(clock_ref.begin()+i);
    clock_size--;
    return;
}

/*
This method should be called when the pin_count of a page becomes 0. 
This method should add the frame containing the unpinned page to the ClockReplacer
*/
void ClockReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(clock_latch_);
    if (find(clock_frames.begin(), clock_frames.end(), frame_id) != clock_frames.end())
        return;
    if (clock_size < total_size){
        clock_frames.push_back(frame_id);
        clock_ref.push_back(true);
        clock_size++;
    }
    //clock_size == total_size
    else {
        frame_id_t instead;
        if (Victim(&instead)){
            for (int32_t i = 0; i < clock_size; ++i)
                if (clock_frames[i] == instead){
                    clock_frames[i] = frame_id;
                    clock_ref[i] = true;
                    // if victim reduces the clock_size by 1, we should add here
                    return;
                }
        }
        else {
            //error?
        }
    }
    return;
}


/*
 This method returns the number of frames that are currently in the ClockReplacer
*/
size_t ClockReplacer::Size() {
    std::lock_guard<std::mutex> lock(clock_latch_);
    return clock_size; 
}

}  // namespace bustub
