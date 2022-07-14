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

#include <fmt/format.h>
#include <parallel_hashmap/phmap.h>

#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/memory/mem_tracker.h"

namespace doris {

using ERRCALLBACK = void (*)();
struct ConsumeErrCallBackInfo {
    std::string cancel_msg;
    bool cancel_task; // Whether to cancel the task when the current tracker exceeds the limit.
    ERRCALLBACK cb_func;

    ConsumeErrCallBackInfo() { init(); }

    ConsumeErrCallBackInfo(const std::string& cancel_msg, bool cancel_task, ERRCALLBACK cb_func)
            : cancel_msg(cancel_msg),
              cancel_task(cancel_task),
              cb_func(cb_func) {}

    void init() {
        cancel_msg = "";
        cancel_task = true;
        cb_func = nullptr;
    }
};

// TCMalloc new/delete Hook is counted in the memory_tracker of the current thread.
//
// In the original design, the MemTracker consume method is called before the memory is allocated.
// If the consume succeeds, the memory is actually allocated, otherwise an exception is thrown.
// But the statistics of memory through TCMalloc new/delete Hook are after the memory is actually allocated,
// which is different from the previous behavior. Therefore, when alloc for some large memory,
// need to manually call consume after stop_mem_tracker, and then start_mem_tracker.
class ThreadMemTrackerMgr {
public:
    ThreadMemTrackerMgr() {}

    ~ThreadMemTrackerMgr() {
        flush_cache<false>();
        _consume_err_cb.init();
        _trackers_stack.clear();
        _trackers_login_count.clear();
    }

    // After thread initialization, calling `init` again must call `clear_untracked_mems` first
    // to avoid memory tracking loss.
    void init();

    // After attach, the current thread TCMalloc Hook starts to consume/release task mem_tracker
    void attach_task(const std::string& cancel_msg, const std::string& task_id,
                     const TUniqueId& fragment_instance_id, MemTrackerLimiter* mem_tracker);

    void detach_task();

    // Must be fast enough! Thread update_tracker may be called very frequently.
    // So for performance, add tracker as early as possible, and then call update_tracker<Existed>.
    void login_observe_tracker(MemTracker* mem_tracker);
    void logout_observe_tracker();

    ConsumeErrCallBackInfo update_consume_err_cb(const std::string& cancel_msg, bool cancel_task,
                                                 ERRCALLBACK cb_func) {
        _temp_consume_err_cb = _consume_err_cb;
        _consume_err_cb.cancel_msg = cancel_msg;
        _consume_err_cb.cancel_task = cancel_task;
        _consume_err_cb.cb_func = cb_func;
        return _temp_consume_err_cb;
    }

    void update_consume_err_cb(const ConsumeErrCallBackInfo& consume_err_cb) {
        _consume_err_cb = consume_err_cb;
    }

    // Note that, If call the memory allocation operation in TCMalloc new/delete Hook,
    // such as calling LOG/iostream/sstream/stringstream/etc. related methods,
    // must increase the control to avoid entering infinite recursion, otherwise it may cause crash or stuck,
    void cache_consume(int64_t size);

    template <bool CheckLimit>
    void flush_cache();

    bool is_attach_task() { return _task_id != ""; }

    MemTrackerLimiter* limiter_mem_tracker() { return _limiter_tracker; }

    void update_check_limit(bool check_limit) { _check_limit = check_limit; }

    void update_stop_no_limiter(bool stop_no_limiter) { _stop_no_limiter = stop_no_limiter; }

    int64_t switch_count = 0;

    std::string print_debug_string() {
        // fmt::memory_buffer mem_trackers_buf;
        // for (const auto& [key, value] : _mem_trackers) {
        //     fmt::format_to(mem_trackers_buf, "{}_{},", std::to_string(key), value->log_usage(1));
        // }
        // fmt::memory_buffer untracked_mems_buf;
        // for (const auto& [key, value] : _untracked_mems) {
        //     fmt::format_to(untracked_mems_buf, "{}_{},", std::to_string(key),
        //                    std::to_string(value));
        // }
        // fmt::memory_buffer mem_tracker_labels_buf;
        // for (const auto& [key, value] : _mem_tracker_labels) {
        //     fmt::format_to(mem_tracker_labels_buf, "{}_{},", std::to_string(key), value);
        // }
        // return fmt::format(
        //         "ThreadMemTrackerMgr debug string, _tracker_id:{}, _untracked_mem:{}, _task_id:{}, "
        //         "_mem_trackers:<{}>, _untracked_mems:<{}>, _mem_tracker_labels:<{}>",
        //         std::to_string(_tracker_id), std::to_string(_untracked_mem), _task_id,
        //         fmt::to_string(mem_trackers_buf), fmt::to_string(untracked_mems_buf),
        //         fmt::to_string(mem_tracker_labels_buf));
    }

private:
    // If tryConsume fails due to task mem tracker exceeding the limit, the task must be canceled
    void exceeded_cancel_task(const std::string& cancel_details);

    void exceeded(int64_t mem_usage, Status st);

private:
    // Cache untracked mem, only update to _untracked_mems when switching mem tracker.
    // Frequent calls to unordered_map _untracked_mems[] in cache_consume will degrade performance.
    int64_t _untracked_mem = 0;
    MemTrackerLimiter* _limiter_tracker;

    // May switch back and forth between multiple trackers frequently. If you use a pointer to save the
    // current tracker, and consume the current untracked mem each time you switch, there is a performance problem:
    //  1. The frequent change of the use-count of shared_ptr has a huge cost; (it can also be solved by using
    //  raw pointers, which requires uniform replacement of the pointers of all mem trackers in doris)
    //  2. The cost of calling consume for the current untracked mem is huge;
    // In order to reduce the cost, during an attach task, the untracked mem of all switched trackers is cached,
    // and the untracked mem is consumed only after the upper limit is reached or when the task is detached.
    // NOTE: flat_hash_map, int replaces string as key, all to improve the speed of map find,
    //  the expected speed is increased by more than 10 times.
    // phmap::flat_hash_map<int64_t, MemTracker*> _observe_trackers_history;
    // phmap::flat_hash_map<int64_t, int64_t> _observe_untracked_mems_history;
    // After the tracker is added to _mem_trackers, if tracker = null is found when using it,
    // we can confirm the tracker label that was added through _mem_tracker_labels.
    // Because for performance, all map keys are tracker id.
    // phmap::flat_hash_map<int64_t, std::string> _observe_tracker_labels_history;
    std::vector<MemTracker*> _trackers_stack;
    phmap::flat_hash_map<MemTracker*, int64_t> _trackers_login_count;

    ConsumeErrCallBackInfo _temp_consume_err_cb;
    // If true, call memtracker try_consume, otherwise call consume.
    bool _check_limit;
    // 
    bool _stop_no_limiter;
    // If there is a memory new/delete operation in the consume method, it may enter infinite recursion.
    bool _stop_consume = false;
    std::string _task_id;
    TUniqueId _fragment_instance_id;
    ConsumeErrCallBackInfo _consume_err_cb;
};

inline void ThreadMemTrackerMgr::init() {
    _task_id = "";
    _consume_err_cb.init();
    _limiter_tracker = MemTrackerLimiter::get_process_tracker_limiter();
    _trackers_stack.clear();
    _trackers_login_count.clear();
    _check_limit = true;
}

inline void ThreadMemTrackerMgr::login_observe_tracker(MemTracker* tracker) {
    DCHECK(tracker) << print_debug_string();
    DCHECK(_trackers_stack.back() != tracker) << print_debug_string();
    _trackers_stack.push_back(tracker);
    if (_trackers_login_count[tracker] == 0) {
        // new add, fulsh untracker mem
        flush_cache<false>();
    }
    _trackers_login_count[tracker] += 1;
}

inline void ThreadMemTrackerMgr::logout_observe_tracker() {
    MemTracker* tracker = _trackers_stack.back();
    _trackers_stack.pop_back();
    _trackers_login_count[tracker] -= 1;
    DCHECK(_trackers_login_count[tracker] >= 0) << print_debug_string();
}

inline void ThreadMemTrackerMgr::cache_consume(int64_t size) {
    _untracked_mem += size;
    // When some threads `0 < _untracked_mem < config::mem_tracker_consume_min_size_bytes`
    // and some threads `_untracked_mem <= -config::mem_tracker_consume_min_size_bytes` trigger consumption(),
    // it will cause tracker->consumption to be temporarily less than 0.
    //
    // Temporary memory may be allocated during the consumption of the mem tracker (in the processing logic of
    // the exceeded limit), which will lead to entering the TCMalloc Hook again, so suspend consumption to avoid
    // falling into an infinite loop.
    if ((_untracked_mem >= config::mem_tracker_consume_min_size_bytes ||
         _untracked_mem <= -config::mem_tracker_consume_min_size_bytes) &&
        !_stop_consume) {
        if (_check_limit) {
            flush_cache<true>();
        } else {
            flush_cache<false>();
        }
    }
}

template <bool CheckLimit>
inline void ThreadMemTrackerMgr::flush_cache() {
    _stop_consume = true;
    if (CheckLimit) {
        Status st = limiter_mem_tracker()->try_consume(_untracked_mem);
        if (!st) {
            // The memory has been allocated, so when TryConsume fails, need to continue to complete
            // the consume to ensure the accuracy of the statistics.
            limiter_mem_tracker()->consume(_untracked_mem);
            exceeded(_untracked_mem, st);
        }
    } else {
        limiter_mem_tracker()->consume(_untracked_mem);
    }
    for (auto tracker_count : _trackers_login_count) {
        if (tracker_count.second != 0) {
            tracker_count.first->consume(_untracked_mem);
        }
    }
    _untracked_mem = 0;
    _stop_consume = false;
}

} // namespace doris
