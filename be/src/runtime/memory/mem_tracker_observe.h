// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "runtime/memory/mem_tracker_base.h"

namespace doris {

class MemTrackerLimiter;

class MemTrackerObserve final : public MemTrackerBase {
public:
    // Cosume/release will not sync to parent.Usually used to manually record the specified memory,
    // It is independent of the recording of TCMalloc Hook in the thread local tracker, so the same
    // block of memory is recorded independently in these two trackers.
    // TODO(zxy) At present, the purpose of most virtual trackers is only to preserve the logic of
    // manually recording memory before, which may be used later. After each virtual tracker is
    // required case by case, discuss its necessity.
    static MemTrackerObserve* create_tracker(const std::string& label,
                                             RuntimeProfile* profile = nullptr);

    ~MemTrackerObserve();

    // Get a temporary tracker with a specified label, and the tracker will be created when the label is first get.
    // Temporary trackers are not automatically destructed, which is usually used for debugging.
    static MemTrackerObserve* get_temporary_mem_tracker(const std::string& label);

    void enable_memory_leak_detection() { _memory_leak_detection = true; }

public:
    void consume(int64_t bytes);

    void release(int64_t bytes) { consume(-bytes); }

    static void batch_consume(int64_t bytes, const std::vector<MemTrackerObserve*>& trackers) {
        for (auto& tracker : trackers) {
            tracker->consume(bytes);
        }
    }

    // Forced transfer, 'dst' may limit exceed, and more ancestor trackers will be updated.
    // /// //
    void transfer_to(MemTrackerObserve* dst, int64_t bytes);

    // Usually, a negative values means that the statistics are not accurate,
    // 1. The released memory is not consumed.
    // 2. The same block of memory, tracker A calls consume, and tracker B calls release.
    // 3. Repeated releases of MemTacker. When the consume is called on the child MemTracker,
    //    after the release is called on the parent MemTracker,
    //    the child ~MemTracker will cause repeated releases.
    static void memory_leak_check(MemTrackerObserve* tracker) {
        DCHECK_EQ(tracker->_consumption->current_value(), 0) << std::endl << tracker->log_usage();
    }

    std::string log_usage(int64_t* logged_consumption = nullptr);

    std::string debug_string() {
        std::stringstream msg;
        msg << "label: " << _label << "; "
            << "consumption: " << _consumption->current_value() << "; "
            << "parent is null: " << ((_parent == nullptr) ? "true" : "false") << "; ";
        return msg.str();
    }

    // Iterator into parent_->_child_observe_trackers for this object. Stored to have O(1) remove.
    std::list<MemTrackerObserve*>::iterator _child_tracker_it;

private:
    MemTrackerObserve(const std::string& label, MemTrackerLimiter* parent, RuntimeProfile* profile)
            : MemTrackerBase(label, parent, profile) {}
};

inline void MemTrackerObserve::consume(int64_t bytes) {
    if (bytes == 0) {
        return;
    } else {
        _consumption->add(bytes);
    }
}

inline void MemTrackerObserve::transfer_to(MemTrackerObserve* dst, int64_t bytes) {
    if (id() == dst->id()) return;
    release(bytes);
    dst->consume(bytes);
}

} // namespace doris
