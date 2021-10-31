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

#include <atomic>
#include <string>

#include "agent/heartbeat_server.h"
#include "common/object_pool.h"
#include "gen_cpp/PaloInternalService_types.h" // for TQueryOptions
#include "gen_cpp/Types_types.h"               // for TUniqueId
#include "runtime/datetime_value.h"
#include "runtime/exec_env.h"
#include "util/threadpool.h"

namespace doris {

// Save the common components of fragments in a query.
// Some components like DescriptorTbl may be very large
// that will slow down each execution of fragments when DeSer them every time.
class DescriptorTbl;
class QueryFragmentsCtx {
public:
    QueryFragmentsCtx(int total_fragment_num, ExecEnv* exec_env)
            : fragment_num(total_fragment_num), timeout_second(-1), _exec_env(exec_env) {
        _start_time = DateTimeValue::local_time();
    }

    ~QueryFragmentsCtx() {};

    bool countdown() { return fragment_num.fetch_sub(1) == 1; }

    bool is_timeout(const DateTimeValue& now) const {
        if (timeout_second <= 0) {
            return false;
        }
        if (now.second_diff(_start_time) > timeout_second) {
            return true;
        }
        return false;
    }

    bool is_coord_restart() const {
        if (HeartbeatServer::is_fe_restart(_exec_env, coord_addr.hostname, _start_time)) {
            LOG(WARNING) << "Coordinator stop or restart: ip" << coord_addr.hostname 
                      << " port:" << coord_addr.port
                      << " query_id:" << query_id;
            return true;
        }
        return false;
    }

    void set_thread_token(int cpu_limit) {
        if (cpu_limit > 0) {
            // For now, cpu_limit will be the max concurrency of the scan thread pool token.
            _thread_token = _exec_env->limited_scan_thread_pool()->new_token(
                    ThreadPool::ExecutionMode::CONCURRENT,
                    cpu_limit);
        }
    }

    ThreadPoolToken* get_token() {
        return _thread_token.get();
    }

public:
    TUniqueId query_id;
    DescriptorTbl* desc_tbl;
    bool set_rsc_info = false;
    std::string user;
    std::string group;
    TNetworkAddress coord_addr;
    TQueryGlobals query_globals;

    /// In the current implementation, for multiple fragments executed by a query on the same BE node,
    /// we store some common components in QueryFragmentsCtx, and save QueryFragmentsCtx in FragmentMgr.
    /// When all Fragments are executed, QueryFragmentsCtx needs to be deleted from FragmentMgr.
    /// Here we use a counter to store the number of Fragments that have not yet been completed,
    /// and after each Fragment is completed, this value will be reduced by one.
    /// When the last Fragment is completed, the counter is cleared, and the worker thread of the last Fragment
    /// will clean up QueryFragmentsCtx.
    std::atomic<int> fragment_num;
    int timeout_second;
    ObjectPool obj_pool;
private:
    ExecEnv* _exec_env;
    DateTimeValue _start_time;

    // A token used to submit olap scanner to the "_limited_scan_thread_pool",
    // This thread pool token is created from "_limited_scan_thread_pool" from exec env.
    // And will be shared by all instances of this query.
    // So that we can control the max thread that a query can be used to execute.
    // If this token is not set, the scanner will be executed in "_scan_thread_pool" in exec env.
    std::unique_ptr<ThreadPoolToken> _thread_token;
};

} // end of namespace

