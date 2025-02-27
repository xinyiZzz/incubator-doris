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

#include "runtime/result_block_buffer.h"

#include <gen_cpp/Data_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <limits>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "arrow/type_fwd.h"
#include "pipeline/dependency.h"
#include "runtime/thread_context.h"
#include "util/runtime_profile.h"
#include "util/thrift_util.h"
#include "vec/core/block.h"
#include "vec/sink/varrow_flight_result_writer.h"
#include "vec/sink/vmysql_result_writer.h"

namespace doris {

template <typename ResultCtxType, typename InBlockType>
ResultBlockBuffer<ResultCtxType, InBlockType>::ResultBlockBuffer(TUniqueId id, int batch_size)
        : _fragment_id(std::move(id)),
          _is_close(false),
          _is_cancelled(false),
          _batch_size(batch_size) {
    _mem_tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::QUERY,
            fmt::format("ResultBlockBuffer#FragmentInstanceId={}", print_id(_fragment_id)));
}

template <typename ResultCtxType, typename InBlockType>
ResultBlockBuffer<ResultCtxType, InBlockType>::~ResultBlockBuffer() {
    cancel(
            Status::Cancelled("ResultBlockBuffer is destructed, this is not the expected path, "
                              "the correct path is "
                              "ResultBufferMgr::cancel before the destructor, fragmentId: {}",
                              print_id(_fragment_id)));
}

template <typename ResultCtxType, typename InBlockType>
Status ResultBlockBuffer<ResultCtxType, InBlockType>::close(const TUniqueId& id,
                                                            Status exec_status) {
    std::unique_lock<std::mutex> l(_lock);
    // close will be called multiple times and error status needs to be collected.
    if (!exec_status.ok()) {
        _status = exec_status;
    }

    auto it = _result_sink_dependencies.find(id);
    if (it != _result_sink_dependencies.end()) {
        it->second->set_always_ready();
        _result_sink_dependencies.erase(it);
    }
    if (!_result_sink_dependencies.empty()) {
        return Status::OK();
    }

    _is_close = true;
    _arrow_data_arrival.notify_all();

    if (!_waiting_rpc.empty()) {
        if (_status.ok()) {
            for (auto& ctx : _waiting_rpc) {
                ctx->on_close(_packet_num, _returned_rows);
            }
        } else {
            for (auto& ctx : _waiting_rpc) {
                ctx->on_failure(_status);
            }
        }
        _waiting_rpc.clear();
    }

    return Status::OK();
}

template <typename ResultCtxType, typename InBlockType>
void ResultBlockBuffer<ResultCtxType, InBlockType>::cancel(const Status& reason) {
    std::unique_lock<std::mutex> l(_lock);
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_mem_tracker);
    _is_cancelled = true;
    _arrow_data_arrival.notify_all();
    for (auto& ctx : _waiting_rpc) {
        ctx->on_failure(reason);
    }
    _waiting_rpc.clear();
    _update_dependency();
    _result_batch_queue.clear();
}

template <typename ResultCtxType, typename InBlockType>
void ResultBlockBuffer<ResultCtxType, InBlockType>::set_dependency(
        const TUniqueId& id, std::shared_ptr<pipeline::Dependency> result_sink_dependency) {
    std::unique_lock<std::mutex> l(_lock);
    _result_sink_dependencies[id] = result_sink_dependency;
    _update_dependency();
}

template <typename ResultCtxType, typename InBlockType>
void ResultBlockBuffer<ResultCtxType, InBlockType>::_update_dependency() {
    if (_is_cancelled) {
        for (auto it : _result_sink_dependencies) {
            it.second->set_ready();
        }
        return;
    }

    for (auto it : _result_sink_dependencies) {
        if (_instance_rows[it.first] > _batch_size) {
            it.second->block();
        } else {
            it.second->set_ready();
        }
    }
}

template class ResultBlockBuffer<vectorized::GetArrowResultBatchCtx, vectorized::Block>;
template class ResultBlockBuffer<vectorized::GetResultBatchCtx, TFetchDataResult>;

} // namespace doris
