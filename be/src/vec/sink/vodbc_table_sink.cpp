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

#include "vec/sink/vodbc_table_sink.h"

#include <sstream>

#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"
#include "vec/core/materialize_block.h"
#include "vec/exprs/vexpr.h"

namespace doris {
namespace vectorized {

VOdbcTableSink::VOdbcTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                               const std::vector<TExpr>& t_exprs)
        : _pool(pool),
          _row_desc(row_desc),
          _t_output_expr(t_exprs),
          _mem_tracker(MemTracker::CreateTracker(-1, "VOdbcTableSink")) {
    _name = "VOdbcTableSink";
}

Status VOdbcTableSink::init(const TDataSink& t_sink) {
    RETURN_IF_ERROR(DataSink::init(t_sink));
    const TOdbcTableSink& t_odbc_sink = t_sink.odbc_table_sink;

    _odbc_param.connect_string = t_odbc_sink.connect_string;
    _odbc_tbl = t_odbc_sink.table;
    _use_transaction = t_odbc_sink.use_transaction;

    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(VExpr::create_expr_trees(_pool, _t_output_expr, &_output_expr_ctxs));
    return Status::OK();
}

Status VOdbcTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(VExpr::prepare(_output_expr_ctxs, state, _row_desc, _mem_tracker));
    std::stringstream title;
    title << "VOdbcTableSink (frag_id=" << state->fragment_instance_id() << ")";
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(title.str()));
    return Status::OK();
}

Status VOdbcTableSink::open(RuntimeState* state) {
    // Prepare the exprs to run.
    RETURN_IF_ERROR(VExpr::open(_output_expr_ctxs, state));

    // create writer
    _writer.reset(new ODBCConnector(_odbc_param));
    RETURN_IF_ERROR(_writer->open());
    if (_use_transaction) {
        RETURN_IF_ERROR(_writer->begin_trans());
    }
    RETURN_IF_ERROR(_writer->init_to_write(_profile));
    return Status::OK();
}

Status VOdbcTableSink::send(RuntimeState* state, RowBatch* batch) {
    return Status::NotSupported(
            "Not Implemented VOdbcTableSink::send(RuntimeState* state, RowBatch* batch)");
}

Status VOdbcTableSink::send(RuntimeState* state, Block* block) {
    Status status = Status::OK();
    if (block == nullptr || block->rows() == 0) {
        return status;
    }

    auto output_block = vectorized::VExprContext::get_output_block_after_execute_exprs(
            _output_expr_ctxs, *block, status);
    materialize_block_inplace(output_block);

    uint32_t start_send_row = 0;
    uint32_t num_row_sent = 0;
    while (start_send_row < output_block.rows()) {
        status = _writer->append(_odbc_tbl, &output_block, _output_expr_ctxs, start_send_row,
                                 &num_row_sent);
        if (UNLIKELY(!status.ok())) return status;
        start_send_row += num_row_sent;
        num_row_sent = 0;
    }

    return Status::OK();
}

Status VOdbcTableSink::close(RuntimeState* state, Status exec_status) {
    VExpr::close(_output_expr_ctxs, state);
    return Status::OK();
}
} // namespace vectorized
} // namespace doris