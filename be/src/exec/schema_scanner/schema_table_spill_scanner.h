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

#include <utility>

#include "exec/schema_scanner/schema_table_spill_manager.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/spill/spill_reader.h"

namespace doris {
#include "common/compile_check_begin.h"

class SchemaTableSpillScanner {
public:
    SchemaTableSpillScanner(TSchemaTableType::type type, vectorized::VExprContextSPtrs expr_ctxs)
            : type_(type), expr_ctxs_(std::move(expr_ctxs)) {}

    ~SchemaTableSpillScanner() = default;

    Status prepare() {
        RETURN_IF_ERROR(SchemaTableSpillManager::instance()->get_time_slices(type_, expr_ctxs_,
                                                                             time_slice_names_));
        return Status::OK();
    }

    Status get_batch(vectorized::Block* output, bool* eos) {
        int cur_batch_size = batch_size_;
        if (offset_ >= time_slice_names_.size()) {
            *eos = true;
            return Status::OK();
        }

        // TODO, new sync thread
        while (offset_ < time_slice_names_.size() && cur_batch_size-- > 0) {
            vectorized::Block* block;
            auto& time_slice_name = time_slice_names_[offset_++];

            // 1. get time slice in stream .
            if (!SchemaTableSpillManager::instance()->get_time_slice_in_stream(
                        type_, time_slice_name, output)) {
                // 2. get spilled time slice.
                vectorized::SpillReaderUPtr reader = std::make_unique<vectorized::SpillReader>(
                        state_->get_query_ctx()->resource_ctx(), stream_id_, time_slice_name);
                RETURN_IF_ERROR(reader->open());
                RETURN_IF_ERROR(reader->read(block, eos));
                output->merge(*block);
            }
        }
        return Status::OK();
    }

private:
    TSchemaTableType::type type_;
    vectorized::VExprContextSPtrs expr_ctxs_;
    std::vector<std::string> time_slice_names_;
    int batch_size_ = 1;
    int offset_ = 0;
};

#include "common/compile_check_end.h"
} // namespace doris
