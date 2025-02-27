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

#include <arrow/type.h>
#include <cctz/time_zone.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/status.h"
#include "runtime/runtime_state.h"

namespace google::protobuf {
class Closure;
} // namespace google::protobuf

namespace brpc {
class Controller;
}

namespace doris {

namespace pipeline {
class Dependency;
} // namespace pipeline

namespace vectorized {
class Block;
} // namespace vectorized

class PFetchDataResult;

class ResultBlockBufferBase {
public:
    ResultBlockBufferBase() = default;
    virtual ~ResultBlockBufferBase() = default;

    template <class TARGET>
    TARGET* cast() {
        DCHECK(dynamic_cast<TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<TARGET*>(this);
    }
    template <class TARGET>
    const TARGET* cast() const {
        DCHECK(dynamic_cast<const TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<const TARGET*>(this);
    }
    virtual Status close(const TUniqueId& id, Status exec_status) = 0;
    virtual void cancel(const Status& reason) = 0;

    [[nodiscard]] virtual const TUniqueId& fragment_id() const = 0;
    [[nodiscard]] virtual std::shared_ptr<MemTrackerLimiter> mem_tracker() = 0;

    virtual void update_return_rows(int64_t num_rows) = 0;
    virtual void set_dependency(const TUniqueId& id,
                                std::shared_ptr<pipeline::Dependency> result_sink_dependency) = 0;
};

// This is used to serialize a result block by normal queries / arrow flight queries / point queries.
template <typename ResultCtxType, typename InBlockType>
class ResultBlockBuffer : public ResultBlockBufferBase {
public:
    ResultBlockBuffer(TUniqueId id, int batch_size);
    ~ResultBlockBuffer() override;

    virtual Status add_batch(RuntimeState* state, std::shared_ptr<InBlockType>& result) = 0;
    virtual void get_batch(ResultCtxType* ctx) = 0;
    Status close(const TUniqueId& id, Status exec_status) override;
    void cancel(const Status& reason) override;

    [[nodiscard]] const TUniqueId& fragment_id() const override { return _fragment_id; }
    [[nodiscard]] std::shared_ptr<MemTrackerLimiter> mem_tracker() override { return _mem_tracker; }

    void update_return_rows(int64_t num_rows) override { _returned_rows.fetch_add(num_rows); }
    void set_dependency(const TUniqueId& id,
                        std::shared_ptr<pipeline::Dependency> result_sink_dependency) override;

protected:
    void _update_dependency();

    using ResultQueue = std::list<std::shared_ptr<InBlockType>>;

    // result's query id
    TUniqueId _fragment_id;
    bool _is_close;
    std::atomic_bool _is_cancelled;
    Status _status;
    // Producer. blocking queue for result batch waiting to sent to FE by _waiting_rpc.
    ResultQueue _result_batch_queue;
    // protects all subsequent data in this block
    std::mutex _lock;

    // get arrow flight result is a sync method, need wait for data ready and return result.
    // TODO, waiting for data will block pipeline, so use a request pool to save requests waiting for data.
    std::condition_variable _arrow_data_arrival;
    // Consumer. RPCs which FE waiting for result. when _fe_result_batch_queue filled, the rpc could be sent.
    std::deque<ResultCtxType*> _waiting_rpc;

    // only used for FE using return rows to check limit
    std::atomic<int64_t> _returned_rows {0};
    // instance id to dependency
    std::unordered_map<TUniqueId, std::shared_ptr<pipeline::Dependency>> _result_sink_dependencies;
    std::unordered_map<TUniqueId, size_t> _instance_rows;
    std::list<std::unordered_map<TUniqueId, size_t>> _instance_rows_in_queue;
    std::shared_ptr<MemTrackerLimiter> _mem_tracker;
    int _packet_num = 0;
    const int _batch_size;
};

} // namespace doris
