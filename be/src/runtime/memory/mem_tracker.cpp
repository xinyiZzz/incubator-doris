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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/mem-tracker.cpp
// and modified by Doris

#include "runtime/memory/mem_tracker.h"

#include <fmt/format.h>

#include "runtime/thread_context.h"
#include "util/pretty_printer.h"
#include "util/string_util.h"
#include "util/time.h"

namespace doris {

const std::string NewMemTracker::COUNTER_NAME = "PeakMemoryUsage";

struct TrackerGroup {
    std::list<NewMemTracker*> trackers;
    std::mutex group_lock;
};

// Save all MemTrackers in use to maintain the weak relationship between NewMemTracker and MemTrackerLimiter.
// When MemTrackerLimiter prints statistics, all NewMemTracker statistics with weak relationship will be printed together.
// Each group corresponds to several MemTrackerLimiters and has a lock.
// Multiple groups are used to reduce the impact of locks.
static std::vector<TrackerGroup> mem_tracker_pool(1000);

NewMemTracker::NewMemTracker(const std::string& label, RuntimeProfile* profile) {
    if (profile == nullptr) {
        _consumption = std::make_shared<RuntimeProfile::HighWaterMarkCounter>(TUnit::BYTES);
    } else {
        // By default, memory consumption is tracked via calls to consume()/release(), either to
        // the tracker itself or to one of its descendents. Alternatively, a consumption metric
        // can be specified, and then the metric's value is used as the consumption rather than
        // the tally maintained by consume() and release(). A tcmalloc metric is used to track
        // process memory consumption, since the process memory usage may be higher than the
        // computed total memory (tcmalloc does not release deallocated memory immediately).
        // Other consumption metrics are used in trackers below the process level to account
        // for memory (such as free buffer pool buffers) that is not tracked by consume() and
        // release().
        _consumption = profile->AddSharedHighWaterMarkCounter(COUNTER_NAME, TUnit::BYTES);
    }

    DCHECK(thread_context()->_thread_mem_tracker_mgr->limiter_mem_tracker_raw() != nullptr);
    MemTrackerLimiter* parent =
            thread_context()->_thread_mem_tracker_mgr->limiter_mem_tracker_raw();
    _label = fmt::format("[Observer] {} | {}", label, parent->label());
    _bind_group_num = parent->group_num();
    {
        std::lock_guard<std::mutex> l(mem_tracker_pool[_bind_group_num].group_lock);
        _tracker_group_it = mem_tracker_pool[_bind_group_num].trackers.insert(
                mem_tracker_pool[_bind_group_num].trackers.end(), this);
    }
}

NewMemTracker::~NewMemTracker() {
    if (_bind_group_num != -1) {
        std::lock_guard<std::mutex> l(mem_tracker_pool[_bind_group_num].group_lock);
        if (_tracker_group_it != mem_tracker_pool[_bind_group_num].trackers.end()) {
            mem_tracker_pool[_bind_group_num].trackers.erase(_tracker_group_it);
            _tracker_group_it = mem_tracker_pool[_bind_group_num].trackers.end();
        }
    }
}

NewMemTracker::Snapshot NewMemTracker::make_snapshot(size_t level) const {
    Snapshot snapshot;
    snapshot.label = split(_label, " | ")[0];
    snapshot.parent = split(_label, " | ")[1];
    snapshot.level = level;
    snapshot.limit = -1;
    snapshot.cur_consumption = _consumption->current_value();
    snapshot.peak_consumption = _consumption->value();
    snapshot.child_count = 0;
    return snapshot;
}

void NewMemTracker::make_group_snapshot(std::vector<NewMemTracker::Snapshot>* snapshots,
                                        size_t level, int64_t group_num,
                                        std::string related_label) {
    std::lock_guard<std::mutex> l(mem_tracker_pool[group_num].group_lock);
    for (auto tracker : mem_tracker_pool[group_num].trackers) {
        if (split(tracker->label(), " | ")[1] == related_label) {
            snapshots->push_back(tracker->make_snapshot(level));
        }
    }
}

std::string NewMemTracker::log_usage(NewMemTracker::Snapshot snapshot) {
    return fmt::format(
            "MemTracker Label={}, Parent Label={}, Used={}({} B), Peak={}({} B)", snapshot.label,
            snapshot.parent, PrettyPrinter::print(snapshot.cur_consumption, TUnit::BYTES),
            snapshot.cur_consumption, PrettyPrinter::print(snapshot.peak_consumption, TUnit::BYTES),
            snapshot.peak_consumption);
}

} // namespace doris
