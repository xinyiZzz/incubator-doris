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

#include "runtime/memory/mem_tracker_limiter.h"

#include <fmt/format.h>

#include <boost/stacktrace.hpp>

#include "gutil/once.h"
#include "gutil/walltime.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/pretty_printer.h"
#include "util/string_util.h"
#include "olap/storage_engine.h"
#include "runtime/load_channel_mgr.h"

namespace doris {

struct TrackerLimiterGroup {
    std::list<MemTrackerLimiter*> trackers;
    std::mutex group_lock;
};

static std::vector<TrackerLimiterGroup> mem_tracker_limiter_pool(1000);

MemTrackerLimiter::MemTrackerLimiter(Type type, int64_t byte_limit, const std::string& label, RuntimeProfile* profile) {
    DCHECK_GE(byte_limit, -1);
    if (profile == nullptr) {
        _consumption = std::make_shared<RuntimeProfile::HighWaterMarkCounter>(TUnit::BYTES);
    } else {
        _consumption = profile->AddSharedHighWaterMarkCounter(COUNTER_NAME, TUnit::BYTES);
    }
    _type = type;
    _label = label;
    _limit = byte_limit;
    _group_num = GetCurrentTimeMicros() % 1000;
    {
        std::lock_guard<std::mutex> l(mem_tracker_limiter_pool[_group_num].group_lock);
        _tracker_limiter_group_it = mem_tracker_limiter_pool[_group_num].trackers.insert(
                mem_tracker_limiter_pool[_group_num].trackers.end(), this);
    }
}

MemTrackerLimiter::~MemTrackerLimiter() {
    // TCMalloc hook will be triggered during destructor memtracker, may cause crash.
    if (_label == "Process") doris::thread_context_ptr.init = false;
    consume_local(_untracked_mem);
    // In order to ensure `consumption of all limiter trackers` + `orphan tracker consumption` = `process tracker consumption`
    // in real time. Merge its consumption into orphan when parent is process, to avoid repetition.
    if (_reset_zero) {
        reset_zero();
        _type = Type::GLOBAL;
    }
    {
        std::lock_guard<std::mutex> l(mem_tracker_limiter_pool[_group_num].group_lock);
        if (_tracker_limiter_group_it != mem_tracker_limiter_pool[_group_num].trackers.end()) {
            mem_tracker_limiter_pool[_group_num].trackers.erase(_tracker_limiter_group_it);
            _tracker_limiter_group_it = mem_tracker_limiter_pool[_group_num].trackers.end();
        }
    }
}

MemTrackerLimiter* MemTrackerLimiter::type_parent_tracker(MemTrackerLimiter::Type type) {
        switch (type) {
        case MemTrackerLimiter::Type::GLOBAL:
            return ExecEnv::GetInstance()->process_mem_tracker().get();
        case MemTrackerLimiter::Type::ORPHAN:
            return ExecEnv::GetInstance()->orphan_mem_tracker_raw();
        case MemTrackerLimiter::Type::QUERY:
            return ExecEnv::GetInstance()->query_pool_mem_tracker().get();
        case MemTrackerLimiter::Type::LOAD_FRAGMENT:
            return ExecEnv::GetInstance()->load_pool_mem_tracker().get();
        case MemTrackerLimiter::Type::LOAD_CHANNEL:
            return ExecEnv::GetInstance()->load_channel_mgr()->mem_tracker();
        case MemTrackerLimiter::Type::COMPACTION:
            return StorageEngine::instance()->compaction_mem_tracker().get();
        default:
            DCHECK(false) << ", type: " << type;
        }
    }

MemTracker::Snapshot MemTrackerLimiter::make_snapshot() const {
    Snapshot snapshot;
    snapshot.type = TypeString[_type];
    snapshot.label = _label;
    snapshot.limit = _limit;
    snapshot.cur_consumption = _consumption->current_value();
    snapshot.peak_consumption = _consumption->value();
    return snapshot;
}

void MemTrackerLimiter::make_process_snapshots(std::vector<MemTracker::Snapshot>* snapshots) {
    for (unsigned i = 0; i < Type::COUNT; ++i) {
        (*snapshots).emplace_back(MemTrackerLimiter::type_parent_tracker(TypeList[i])->make_snapshot());
    }
    for(unsigned i = 0; i < mem_tracker_limiter_pool.size(); ++i) {
        std::lock_guard<std::mutex> l(mem_tracker_limiter_pool[i].group_lock);
        for (auto tracker : mem_tracker_limiter_pool[i].trackers) {
            (*snapshots).emplace_back(tracker->make_snapshot());
        }
    }
}

void MemTrackerLimiter::make_type_snapshots(std::vector<MemTracker::Snapshot>* snapshots, MemTrackerLimiter::Type type) {
    for(unsigned i = 0; i < mem_tracker_limiter_pool.size(); ++i) {
        std::lock_guard<std::mutex> l(mem_tracker_limiter_pool[i].group_lock);
        for (auto tracker : mem_tracker_limiter_pool[i].trackers) {
            if (tracker->type() == type) {
                (*snapshots).emplace_back(tracker->make_snapshot());
                 MemTracker::make_group_snapshot(snapshots, tracker->group_num(), tracker->label());
            }
        }
    }
}

// Calling this on the query tracker results in output like:
//
//  Query(4a4c81fedaed337d:4acadfda00000000) Limit=10.00 GB Total=508.28 MB Peak=508.45 MB
//    Fragment 4a4c81fedaed337d:4acadfda00000000: Total=8.00 KB Peak=8.00 KB
//      EXCHANGE_NODE (id=4): Total=0 Peak=0
//      DataStreamRecvr: Total=0 Peak=0
//    Block Manager: Limit=6.68 GB Total=394.00 MB Peak=394.00 MB
//    Fragment 4a4c81fedaed337d:4acadfda00000006: Total=233.72 MB Peak=242.24 MB
//      AGGREGATION_NODE (id=1): Total=139.21 MB Peak=139.84 MB
//      HDFS_SCAN_NODE (id=0): Total=93.94 MB Peak=102.24 MB
//      DataStreamSender (dst_id=2): Total=45.99 KB Peak=85.99 KB
//    Fragment 4a4c81fedaed337d:4acadfda00000003: Total=274.55 MB Peak=274.62 MB
//      AGGREGATION_NODE (id=3): Total=274.50 MB Peak=274.50 MB
//      EXCHANGE_NODE (id=2): Total=0 Peak=0
//      DataStreamRecvr: Total=45.91 KB Peak=684.07 KB
//      DataStreamSender (dst_id=4): Total=680.00 B Peak=680.00 B
//
// If 'reservation_metrics_' are set, we ge a more granular breakdown:
//   TrackerName: Limit=5.00 MB Reservation=5.00 MB OtherMemory=1.04 MB
//                Total=6.04 MB Peak=6.45 MB
//
std::string MemTrackerLimiter::log_usage() {
    int64_t curr_consumption = consumption();
    int64_t peak_consumption = _consumption->value();
    std::string detail =
            "MemTrackerLimiter Label={}, Limit={}({} B), Used={}({} B), Peak={}({} B), Exceeded={}";
    detail = fmt::format(detail, _label, print_bytes(_limit), _limit, print_bytes(curr_consumption),
                         curr_consumption, print_bytes(peak_consumption), peak_consumption,
                         limit_exceeded() ? "true" : "false");
    return detail;
}

std::string MemTrackerLimiter::log_process_usage() {
    DCHECK(_label == "Process");
    std::string detail;
    for (unsigned i = 0; i < Type::COUNT; ++i) {
        detail += "\n    " + type_parent_tracker(TypeList[i])->log_usage();
    }
    return detail;
}

std::string MemTrackerLimiter::log_type_usage() {
    DCHECK(_type == Type::GLOBAL);
    std::string detail = log_usage();
    std::string child_trackers_usage;
    std::vector<MemTracker::Snapshot> snapshots;
    MemTrackerLimiter::make_type_snapshots(&snapshots, _type);
    for (const auto& snapshot : snapshots) {
        child_trackers_usage += "\n    " + MemTracker::log_usage(snapshot);
    }
    if (!child_trackers_usage.empty()) detail += child_trackers_usage;
    return detail;
}

std::string MemTrackerLimiter::log_task_usage() {
    DCHECK(_type != Type::GLOBAL);
    std::string detail = log_usage();
    std::string child_trackers_usage;
    std::vector<MemTracker::Snapshot> snapshots;
    MemTracker::make_group_snapshot(&snapshots, _group_num, _label);
    for (const auto& snapshot : snapshots) {
        child_trackers_usage += "\n    " + MemTracker::log_usage(snapshot);
    }
    if (!child_trackers_usage.empty()) detail += child_trackers_usage;
    return detail;
}

void MemTrackerLimiter::print_log_usage(const std::string& msg) {
    std::string detail = msg;
    detail += "\n    " + fmt::format(
                                 "process memory used {}, limit {}, hard limit {}, tc/jemalloc "
                                 "allocator cache {}",
                                 PerfCounters::get_vm_rss_str(), MemInfo::mem_limit_str(),
                                 print_bytes(MemInfo::hard_mem_limit()),
                                 MemInfo::allocator_cache_mem_str());
    if (_print_log_usage) {
        if (_label == "Process") {
            detail += log_process_usage();
        } else if (_type == Type::GLOBAL) {
            detail += log_type_usage();
        } else {
            detail += log_task_usage();
        }
        // TODO: memory leak by calling `boost::stacktrace` in tcmalloc hook,
        // test whether overwriting malloc/free is the same problem in jemalloc/tcmalloc.
        // detail += "\n" + boost::stacktrace::to_string(boost::stacktrace::stacktrace());
        LOG(WARNING) << detail;
        _print_log_usage = false;
    }
}

std::string MemTrackerLimiter::mem_limit_exceeded(const std::string& msg,
                                                  const std::string& limit_exceeded_errmsg_prefix) {
    DCHECK(_limit != -1);
    STOP_CHECK_THREAD_MEM_TRACKER_LIMIT();
    std::string detail = fmt::format("Memory limit exceeded:<consuming tracker:<{}>, {}>, executing msg:<{}>",
                        _label, limit_exceeded_errmsg_prefix, msg);
    auto failed_msg = MemTrackerLimiter::limit_exceeded_errmsg_suffix_str(detail);
    print_log_usage(failed_msg);
    return failed_msg;
}

Status MemTrackerLimiter::mem_limit_exceeded(RuntimeState* state, const std::string& msg,
                                             int64_t failed_alloc_size) {
    auto failed_msg = mem_limit_exceeded(msg, limit_exceeded_errmsg_prefix_str(failed_alloc_size, this));
    state->log_error(failed_msg);
    return Status::MemoryLimitExceeded(failed_msg);
}

} // namespace doris
