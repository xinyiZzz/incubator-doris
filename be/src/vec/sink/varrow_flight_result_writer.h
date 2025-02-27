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

#include "common/status.h"
#include "runtime/result_block_buffer.h"
#include "runtime/result_writer.h"
#include "util/runtime_profile.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;
class PFetchArrowDataResult;

namespace vectorized {
class Block;

struct GetArrowResultBatchCtx {
    brpc::Controller* cntl = nullptr;
    PFetchArrowDataResult* result = nullptr;
    google::protobuf::Closure* done = nullptr;

    GetArrowResultBatchCtx(brpc::Controller* cntl_, PFetchArrowDataResult* result_,
                           google::protobuf::Closure* done_)
            : cntl(cntl_), result(result_), done(done_) {}

    void on_failure(const Status& status);
    void on_close(int64_t packet_seq, int64_t /* returned_rows */);
    void on_data(const std::shared_ptr<vectorized::Block>& block, int64_t packet_seq,
                 int be_exec_version,
                 segment_v2::CompressionTypePB fragment_transmission_compression_type,
                 std::string timezone, RuntimeProfile::Counter* serialize_batch_ns_timer,
                 RuntimeProfile::Counter* uncompressed_bytes_counter,
                 RuntimeProfile::Counter* compressed_bytes_counter);
};

class ArrowFlightResultBlockBuffer final
        : public ResultBlockBuffer<GetArrowResultBatchCtx, vectorized::Block> {
public:
    ArrowFlightResultBlockBuffer(TUniqueId id, RuntimeState* state)
            : ResultBlockBuffer<GetArrowResultBatchCtx, vectorized::Block>(id, state->batch_size()),
              _profile("ResultBlockBuffer " + print_id(_fragment_id)),
              _timezone(state->timezone()),
              _timezone_obj(state->timezone_obj()),
              _be_exec_version(state->be_exec_version()),
              _fragment_transmission_compression_type(
                      state->fragement_transmission_compression_type()) {
        _serialize_batch_ns_timer = ADD_TIMER(&_profile, "SerializeBatchNsTime");
        _uncompressed_bytes_counter = ADD_COUNTER(&_profile, "UncompressedBytes", TUnit::BYTES);
        _compressed_bytes_counter = ADD_COUNTER(&_profile, "CompressedBytes", TUnit::BYTES);
    }
    ~ArrowFlightResultBlockBuffer() override = default;
    void get_batch(GetArrowResultBatchCtx* ctx) override;
    Status add_batch(RuntimeState* state, std::shared_ptr<vectorized::Block>& result) override;
    Status get_arrow_batch(std::shared_ptr<vectorized::Block>* result);
    void get_timezone(cctz::time_zone& timezone_obj) { timezone_obj = _timezone_obj; }
    void register_schema(const std::shared_ptr<arrow::Schema>& arrow_schema);
    Status find_schema(std::shared_ptr<arrow::Schema>* arrow_schema);

private:
    std::shared_ptr<arrow::Schema> _arrow_schema;
    // only used for ArrowFlightBatchRemoteReader
    RuntimeProfile _profile;
    RuntimeProfile::Counter* _serialize_batch_ns_timer = nullptr;
    RuntimeProfile::Counter* _uncompressed_bytes_counter = nullptr;
    RuntimeProfile::Counter* _compressed_bytes_counter = nullptr;
    std::string _timezone;
    cctz::time_zone _timezone_obj;
    const int _be_exec_version;
    const segment_v2::CompressionTypePB _fragment_transmission_compression_type;
};

class VArrowFlightResultWriter final : public ResultWriter {
public:
    VArrowFlightResultWriter(ResultBlockBufferBase* sinker,
                             const VExprContextSPtrs& output_vexpr_ctxs,
                             RuntimeProfile* parent_profile);

    Status init(RuntimeState* state) override;

    Status write(RuntimeState* state, Block& block) override;

    Status close(Status) override;

private:
    void _init_profile();

    ArrowFlightResultBlockBuffer* _sinker = nullptr;

    const VExprContextSPtrs& _output_vexpr_ctxs;

    RuntimeProfile* _parent_profile = nullptr; // parent profile from result sink. not owned
    // total time cost on append batch operation
    RuntimeProfile::Counter* _append_row_batch_timer = nullptr;
    // file write timer, child timer of _append_row_batch_timer
    RuntimeProfile::Counter* _result_send_timer = nullptr;
    // number of sent rows
    RuntimeProfile::Counter* _sent_rows_counter = nullptr;
    // size of sent data
    RuntimeProfile::Counter* _bytes_sent_counter = nullptr;
    // If true, no block will be sent
    bool _is_dry_run = false;

    uint64_t _bytes_sent = 0;
};
} // namespace vectorized
} // namespace doris

#include "common/compile_check_end.h"
