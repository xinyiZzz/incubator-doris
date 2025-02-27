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
#include <gen_cpp/PaloInternalService_types.h>
#include <stddef.h>

#include <memory>
#include <vector>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/result_block_buffer.h"
#include "runtime/result_writer.h"
#include "util/mysql_row_buffer.h"
#include "util/runtime_profile.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

namespace vectorized {
class Block;

struct GetResultBatchCtx {
    brpc::Controller* cntl = nullptr;
    PFetchDataResult* result = nullptr;
    google::protobuf::Closure* done = nullptr;

    GetResultBatchCtx(brpc::Controller* cntl_, PFetchDataResult* result_,
                      google::protobuf::Closure* done_)
            : cntl(cntl_), result(result_), done(done_) {}

    void on_failure(const Status& status);
    void on_close(int64_t packet_seq, int64_t returned_rows = 0);
    void on_data(const std::shared_ptr<TFetchDataResult>& t_result, int64_t packet_seq,
                 bool eos = false);
};

class NormalResultBlockBuffer : public ResultBlockBuffer<GetResultBatchCtx, TFetchDataResult> {
public:
    NormalResultBlockBuffer(TUniqueId id, int buffer_size, RuntimeState* state)
            : ResultBlockBuffer<GetResultBatchCtx, TFetchDataResult>(id, state->batch_size()),
              _buffer_limit(buffer_size) {}
    ~NormalResultBlockBuffer() override = default;
    void get_batch(GetResultBatchCtx* ctx) override;
    Status add_batch(RuntimeState* state, std::shared_ptr<TFetchDataResult>& result) override;

protected:
    NormalResultBlockBuffer()
            : ResultBlockBuffer<GetResultBatchCtx, TFetchDataResult>(TUniqueId(), 0),
              _buffer_limit(0) {}

private:
    const int _buffer_limit;
};

template <bool is_binary_format = false>
class VMysqlResultWriter final : public ResultWriter {
public:
    VMysqlResultWriter(ResultBlockBufferBase* sinker, const VExprContextSPtrs& output_vexpr_ctxs,
                       RuntimeProfile* parent_profile);

    Status init(RuntimeState* state) override;

    Status write(RuntimeState* state, Block& block) override;

    Status close(Status status) override;

private:
    void _init_profile();

    Status _set_options(const TSerdeDialect::type& serde_dialect);

    template <PrimitiveType type, bool is_nullable>
    Status _add_one_column(const ColumnPtr& column_ptr, std::unique_ptr<TFetchDataResult>& result,
                           std::vector<MysqlRowBuffer<is_binary_format>>& rows_buffer,
                           bool arg_const, int scale = -1,
                           const DataTypes& sub_types = DataTypes());
    int _add_one_cell(const ColumnPtr& column_ptr, size_t row_idx, const DataTypePtr& type,
                      MysqlRowBuffer<is_binary_format>& buffer, int scale = -1);

    Status _write_one_block(RuntimeState* state, Block& block);

    NormalResultBlockBuffer* _sinker = nullptr;

    const VExprContextSPtrs& _output_vexpr_ctxs;

    RuntimeProfile* _parent_profile; // parent profile from result sink. not owned
    // total time cost on append batch operation
    RuntimeProfile::Counter* _append_row_batch_timer = nullptr;
    // tuple convert timer, child timer of _append_row_batch_timer
    RuntimeProfile::Counter* _convert_tuple_timer = nullptr;
    // file write timer, child timer of _append_row_batch_timer
    RuntimeProfile::Counter* _result_send_timer = nullptr;
    // timer of copying buffer to thrift
    RuntimeProfile::Counter* _copy_buffer_timer = nullptr;
    // number of sent rows
    RuntimeProfile::Counter* _sent_rows_counter = nullptr;
    // size of sent data
    RuntimeProfile::Counter* _bytes_sent_counter = nullptr;
    // If true, no block will be sent
    bool _is_dry_run = false;

    uint64_t _bytes_sent = 0;

    DataTypeSerDe::FormatOptions _options;
};
} // namespace vectorized
} // namespace doris

#include "common/compile_check_end.h"
