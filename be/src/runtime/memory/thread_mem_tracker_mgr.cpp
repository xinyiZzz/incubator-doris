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

#include "runtime/memory/thread_mem_tracker_mgr.h"

#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "service/backend_options.h"

namespace doris {

void ThreadMemTrackerMgr::attach_limiter_tracker(
        const std::shared_ptr<MemTrackerLimiter>& mem_tracker,
        const TUniqueId& fragment_instance_id) {
    DCHECK(mem_tracker);
    flush_untracked_mem<false>();
    _fragment_instance_id = fragment_instance_id;
    _limiter_tracker = mem_tracker;
    _limiter_tracker_raw = mem_tracker.get();
}

void ThreadMemTrackerMgr::detach_limiter_tracker(
        const std::shared_ptr<MemTrackerLimiter>& old_mem_tracker) {
    flush_untracked_mem<false>();
    _fragment_instance_id = TUniqueId();
    _limiter_tracker = old_mem_tracker;
    _limiter_tracker_raw = old_mem_tracker.get();
}

void ThreadMemTrackerMgr::exceeded_cancel_task(const std::string& cancel_details) {
    if (_fragment_instance_id != TUniqueId()) {
        ExecEnv::GetInstance()->fragment_mgr()->cancel(
                _fragment_instance_id, PPlanFragmentCancelReason::MEMORY_LIMIT_EXCEED,
                cancel_details);
    }
}

void ThreadMemTrackerMgr::exceeded() {
    if (_cb_func != nullptr) {
        _cb_func();
    }
    if (is_attach_query()) {
        exceeded_cancel_task(_exceed_mem_limit_msg);
    }
    _check_limit = false; // Make sure it will only be canceled once
}

// void ThreadMemTrackerMgr::log_backtrace() {
//     // int size = 0;
//     // void *buffer[BACKTRACE_SIZE];
//     // size = backtrace(buffer, BACKTRACE_SIZE);
    
//     // std::string log_path = std::string(getenv("DORIS_HOME")) + "/log/be.INFO";
//     // auto fp_closer = [](FILE* fp) { fclose(fp); };
//     // std::unique_ptr<FILE, decltype(fp_closer)> fp(fopen(log_path.c_str(), "a"), fp_closer);
//     // if (fp == nullptr) {
//     //     LOG(WARNING) << "open file failed, file=" << log_path;
//     // }

//     // backtrace_symbols_fd(buffer, size, fileno(fp.get()));

//     // LOG(WARNING) << boost::stacktrace::to_string(boost::stacktrace::stacktrace());

//     // auto res = fwrite(buffer, length, 1, fp.get());
//     // if (res != 1) {
//     //     LOG(WARNING) << "fail to write data to file, file=" << local_path
//     //                 << ", error=" << ferror(fp.get());
//     //     status = Status::InternalError("fail to write data when download");
//     //     return false;
//     // }

//     // FILE* file = fopen(filename, "w");
//     // EXPECT_TRUE(file != nullptr);
//     // fwrite(data, 1, strlen(data), file);
//     // fclose(file);
    
//     // backtrace_symbols_fd(buffer, size, STDOUT_FILENO);
// }

} // namespace doris
