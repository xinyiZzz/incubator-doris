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

#include "vec/common/allocator.h"

#include <glog/logging.h>

#include <atomic>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <memory>
#include <new>
#include <thread>

// Allocator is used by too many files. For compilation speed, put dependencies in `.cpp` as much as possible.
#include "runtime/fragment_mgr.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/memory/thread_mem_tracker_mgr.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"
#include "util/mem_info.h"
#include "util/stack_util.h"
#include "util/uid_util.h"

std::unordered_map<void*, size_t> RecordSizeMemoryAllocator::_allocated_sizes;
std::mutex RecordSizeMemoryAllocator::_mutex;

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator>
void Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator>::sys_memory_check(
        size_t size) const {
#ifdef BE_TEST
    if (!doris::ExecEnv::ready()) {
        return;
    }
#endif
    if (doris::thread_context()->skip_memory_check != 0) {
        return;
    }
    if (doris::GlobalMemoryArbitrator::is_exceed_hard_mem_limit(size)) {
        // Only thread attach query, and has not completely waited for thread_wait_gc_max_milliseconds,
        // will wait for gc, asynchronous cancel or throw bad::alloc.
        // Otherwise, if the external catch, directly throw bad::alloc.
        std::string err_msg;
        err_msg += fmt::format(
                "Allocator sys memory check failed: Cannot alloc:{}, consuming "
                "tracker:<{}>, peak used {}, current used {}, exec node:<{}>, {}.",
                size, doris::thread_context()->thread_mem_tracker()->label(),
                doris::thread_context()->thread_mem_tracker()->peak_consumption(),
                doris::thread_context()->thread_mem_tracker()->consumption(),
                doris::thread_context()->thread_mem_tracker_mgr->last_consumer_tracker(),
                doris::GlobalMemoryArbitrator::process_limit_exceeded_errmsg_str());

        if (doris::config::stacktrace_in_alloc_large_memory_bytes > 0 &&
            size > doris::config::stacktrace_in_alloc_large_memory_bytes) {
            err_msg += "\nAlloc Stacktrace:\n" + doris::get_stack_trace();
        }

        // TODO, Save the query context in the thread context, instead of finding whether the query id is canceled in fragment_mgr.
        if (doris::thread_context()->thread_mem_tracker_mgr->is_query_cancelled()) {
            if (doris::enable_thread_catch_bad_alloc) {
                throw doris::Exception(doris::ErrorCode::MEM_ALLOC_FAILED, err_msg);
            }
            return;
        }

        // no significant impact on performance is expected.
        doris::MemInfo::notify_je_purge_dirty_pages();

        if (doris::thread_context()->thread_mem_tracker_mgr->is_attach_query() &&
            doris::thread_context()->thread_mem_tracker_mgr->wait_gc()) {
            int64_t wait_milliseconds = 0;
            LOG(INFO) << fmt::format(
                    "Query:{} waiting for enough memory in thread id:{}, maximum {}ms, {}.",
                    print_id(doris::thread_context()->task_id()),
                    doris::thread_context()->get_thread_id(),
                    doris::config::thread_wait_gc_max_milliseconds, err_msg);
            // only query thread exceeded memory limit for the first time and wait_gc is true.
            doris::MemInfo::je_thread_tcache_flush();
            if (!doris::config::disable_memory_gc) {
                while (wait_milliseconds < doris::config::thread_wait_gc_max_milliseconds) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    if (!doris::GlobalMemoryArbitrator::is_exceed_hard_mem_limit(size)) {
                        doris::GlobalMemoryArbitrator::refresh_interval_memory_growth += size;
                        break;
                    }
                    if (doris::thread_context()->thread_mem_tracker_mgr->is_query_cancelled()) {
                        if (doris::enable_thread_catch_bad_alloc) {
                            throw doris::Exception(doris::ErrorCode::MEM_ALLOC_FAILED, err_msg);
                        }
                        return;
                    }
                    wait_milliseconds += 100;
                }
            }
            if (wait_milliseconds >= doris::config::thread_wait_gc_max_milliseconds) {
                // Make sure to completely wait thread_wait_gc_max_milliseconds only once.
                doris::thread_context()->thread_mem_tracker_mgr->disable_wait_gc();
                doris::MemTrackerLimiter::print_log_process_usage();
                // If the external catch, throw bad::alloc first, let the query actively cancel. Otherwise asynchronous cancel.
                if (!doris::enable_thread_catch_bad_alloc) {
#ifndef NDEBUG
                    err_msg += "\nAlloc Stacktrace:\n" + doris::get_stack_trace();
#endif
                    LOG(INFO) << fmt::format(
                            "Query:{} canceled asyn, after waiting for memory {}ms, {}.",
                            print_id(doris::thread_context()->task_id()), wait_milliseconds,
                            err_msg);
                    doris::thread_context()->thread_mem_tracker_mgr->cancel_query(err_msg);
                } else {
                    LOG(INFO) << fmt::format(
                            "Query:{} throw exception, after waiting for memory {}ms, {}.",
                            print_id(doris::thread_context()->task_id()), wait_milliseconds,
                            err_msg);
                    throw doris::Exception(doris::ErrorCode::MEM_ALLOC_FAILED, err_msg);
                }
            }
            // else, enough memory is available, the query continues execute.
        } else if (doris::enable_thread_catch_bad_alloc) {
            LOG(INFO) << fmt::format("sys memory check failed, throw exception, {}.", err_msg);
            doris::MemTrackerLimiter::print_log_process_usage();
            throw doris::Exception(doris::ErrorCode::MEM_ALLOC_FAILED, err_msg);
        } else {
#ifndef NDEBUG
            err_msg += "\nAlloc Stacktrace:\n" + doris::get_stack_trace();
#endif
            LOG(INFO) << fmt::format("sys memory check failed, no throw exception, {}.", err_msg);
        }
    }
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator>
void Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator>::memory_tracker_check(
        size_t size) const {
#ifdef BE_TEST
    if (!doris::ExecEnv::ready()) {
        return;
    }
#endif
    if (doris::thread_context()->skip_memory_check != 0) {
        return;
    }
    auto st = doris::thread_context()->thread_mem_tracker()->check_limit(size);
    if (!st) {
        auto err_msg = fmt::format("Allocator mem tracker check failed, {}", st.to_string());
        doris::thread_context()->thread_mem_tracker()->print_log_usage(err_msg);
        // If the external catch, throw bad::alloc first, let the query actively cancel. Otherwise asynchronous cancel.
        if (doris::thread_context()->thread_mem_tracker_mgr->is_attach_query()) {
            doris::thread_context()->thread_mem_tracker_mgr->disable_wait_gc();
            if (!doris::enable_thread_catch_bad_alloc) {
                LOG(INFO) << fmt::format("query/load:{} canceled asyn, {}.",
                                         print_id(doris::thread_context()->task_id()), err_msg);
                doris::thread_context()->thread_mem_tracker_mgr->cancel_query(err_msg);
            } else {
                LOG(INFO) << fmt::format("query/load:{} throw exception, {}.",
                                         print_id(doris::thread_context()->task_id()), err_msg);
                throw doris::Exception(doris::ErrorCode::MEM_ALLOC_FAILED, err_msg);
            }
        } else if (doris::enable_thread_catch_bad_alloc) {
            LOG(INFO) << fmt::format("memory tracker check failed, throw exception, {}.", err_msg);
            throw doris::Exception(doris::ErrorCode::MEM_ALLOC_FAILED, err_msg);
        } else {
            LOG(INFO) << fmt::format("memory tracker check failed, no throw exception, {}.",
                                     err_msg);
        }
    }
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator>
void Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator>::memory_check(
        size_t size) const {
    sys_memory_check(size);
    memory_tracker_check(size);
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator>
void Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator>::consume_memory(
        size_t size) {
    // Usually, an object that inherits Allocator has the same TLS tracker for each alloc.
    // If an object that inherits Allocator needs to be reused by multiple queries,
    // it is necessary to switch the same tracker to TLS when calling alloc.
    // However, in ORC Reader, ORC DataBuffer will be reused, but we cannot switch TLS tracker,
    // so we update the Allocator tracker when the TLS tracker changes.
    // note that the tracker in thread context when object that inherit Allocator is constructed may be
    // no attach memory tracker in tls. usually the memory tracker is attached in tls only during the first alloc.
    if (mem_tracker_ == nullptr ||
        mem_tracker_->label() != doris::thread_context()->thread_mem_tracker()->label()) {
        mem_tracker_ = doris::thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker();
    }
    CONSUME_THREAD_MEM_TRACKER(size);
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator>
void Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator>::release_memory(
        size_t size) const {
    doris::ThreadContext* thread_context = doris::thread_context(true);
    if ((thread_context && thread_context->thread_mem_tracker()->label() != "Orphan") ||
        mem_tracker_ == nullptr) {
        // If thread_context exist and the label of thread_mem_tracker not equal to `Orphan`,
        // this means that in the scope of SCOPED_ATTACH_TASK,
        // so thread_mem_tracker should be used to release memory.
        // If mem_tracker_ is nullptr there is a scenario where an object that inherits Allocator
        // has never called alloc, but free memory.
        // in phmap, the memory alloced by an object may be transferred to another object and then free.
        // in this case, thread context must attach a memory tracker other than Orphan,
        // otherwise memory tracking will be wrong.
        RELEASE_THREAD_MEM_TRACKER(size);
    } else {
        // if thread_context does not exist or the label of thread_mem_tracker is equal to
        // `Orphan`, it usually happens during object destruction. This means that
        // the scope of SCOPED_ATTACH_TASK has been left,  so release memory using Allocator tracker.
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(mem_tracker_);
        RELEASE_THREAD_MEM_TRACKER(size);
    }
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator>
void Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator>::throw_bad_alloc(
        const std::string& err) const {
    LOG(WARNING) << err
                 << fmt::format("{}, Stacktrace: {}",
                                doris::GlobalMemoryArbitrator::process_mem_log_str(),
                                doris::get_stack_trace());
    doris::MemTrackerLimiter::print_log_process_usage();
    throw doris::Exception(doris::ErrorCode::MEM_ALLOC_FAILED, err);
}

#ifndef NDEBUG
template <bool clear_memory_, bool mmap_populate, bool use_mmap>
void Allocator<clear_memory_, mmap_populate, use_mmap>::add_address_sanitizers(void* buf,
                                                                               size_t size) const {
#ifdef BE_TEST
    if (!doris::ExecEnv::ready()) {
        return;
    }
#endif
    doris::thread_context()->thread_mem_tracker()->add_address_sanitizers(buf, size);
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap>
void Allocator<clear_memory_, mmap_populate, use_mmap>::remove_address_sanitizers(
        void* buf, size_t size) const {
#ifdef BE_TEST
    if (!doris::ExecEnv::ready()) {
        return;
    }
#endif
    doris::thread_context()->thread_mem_tracker()->remove_address_sanitizers(buf, size);
}
#endif

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator>
void* Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator>::alloc(size_t size,
                                                                                size_t alignment) {
    return alloc_impl(size, alignment);
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator>
void* Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator>::realloc(
        void* buf, size_t old_size, size_t new_size, size_t alignment) {
    return realloc_impl(buf, old_size, new_size, alignment);
}

template class Allocator<true, true, true, DefaultMemoryAllocator>;
template class Allocator<true, true, false, DefaultMemoryAllocator>;
template class Allocator<true, false, true, DefaultMemoryAllocator>;
template class Allocator<true, false, false, DefaultMemoryAllocator>;
template class Allocator<false, true, true, DefaultMemoryAllocator>;
template class Allocator<false, true, false, DefaultMemoryAllocator>;
template class Allocator<false, false, true, DefaultMemoryAllocator>;
template class Allocator<false, false, false, DefaultMemoryAllocator>;

/** It would be better to put these Memory Allocators where they are used, such as in the orc memory pool and arrow memory pool.
  * But currently allocators use templates in .cpp instead of all in .h, so they can only be placed here.
  */
template class Allocator<true, true, false, ORCMemoryAllocator>;
template class Allocator<true, false, true, ORCMemoryAllocator>;
template class Allocator<true, false, false, ORCMemoryAllocator>;
template class Allocator<false, true, true, ORCMemoryAllocator>;
template class Allocator<false, true, false, ORCMemoryAllocator>;
template class Allocator<false, false, true, ORCMemoryAllocator>;
template class Allocator<false, false, false, ORCMemoryAllocator>;

template class Allocator<true, true, true, RecordSizeMemoryAllocator>;
template class Allocator<true, true, false, RecordSizeMemoryAllocator>;
template class Allocator<true, false, true, RecordSizeMemoryAllocator>;
template class Allocator<true, false, false, RecordSizeMemoryAllocator>;
template class Allocator<false, true, true, RecordSizeMemoryAllocator>;
template class Allocator<false, true, false, RecordSizeMemoryAllocator>;
template class Allocator<false, false, true, RecordSizeMemoryAllocator>;
template class Allocator<false, false, false, RecordSizeMemoryAllocator>;
