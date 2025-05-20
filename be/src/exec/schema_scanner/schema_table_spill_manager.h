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

#include <string>
#include <unordered_map>
#include <utility>

#include "runtime/define_primitive_type.h"
#include "util/date_func.h"
#include "util/time.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/spill/spill_writer.h"

namespace doris {
#include "common/compile_check_begin.h"

class SchemaTableSpillManager {
public:
    struct SchemaTableTimeSlice {
        SchemaTableTimeSlice(std::string datetime, const std::shared_ptr<vectorized::Block>& block)
                : datetime_(std::move(datetime)), block_(block) {}

        std::string datetime_;
        std::shared_ptr<vectorized::Block> block_;
    };

    struct SchemaTableFetchTask {
        SchemaTableFetchTask(size_t fetch_interval_ms, size_t ttl_s,
                             std::function<std::shared_ptr<vectorized::Block>()> fetch_function)
                : fetch_interval_ms_(fetch_interval_ms),
                  last_fetch_interval_ms_(fetch_interval_ms),
                  ttl_s_(ttl_s),
                  fetch_function_(std::move(fetch_function)) {}

        size_t fetch_interval_ms_;
        size_t last_fetch_interval_ms_;
        size_t ttl_s_;
        std::function<std::shared_ptr<vectorized::Block>()> fetch_function_;
    };

    static SchemaTableSpillManager* create_global_instance() {
        return new SchemaTableSpillManager();
    }
    static SchemaTableSpillManager* instance() {
        return ExecEnv::GetInstance()->get_schema_table_spill_manager();
    }

    static std::string schema_table_spill_path(TSchemaTableType::type type) {
        switch (type) {
        case TSchemaTableType::SCH_BACKEND_ACTIVE_TASKS:
            return "BackendActiveTasks";
        case TSchemaTableType::SCH_PROCESSLIST:
            return "ProcessList";
        default:
            throw Exception(Status::FatalError("not match type of schema table_ spill :{}",
                                               static_cast<int>(type)));
        }
        throw Exception(Status::FatalError("__builtin_unreachable"));
    }

    SchemaTableSpillManager() : stop_threads_latch_(1) {}

    ~SchemaTableSpillManager() {
        LOG(INFO) << "SchemaTableSpillManager is stopping";
        DCHECK(stop_threads_latch_.count() != 0);
        stop_threads_latch_.count_down();
        for (auto&& t : threads_) {
            if (t) {
                t->join();
            }
        }
        LOG(INFO) << "SchemaTableSpillManager stopped after background threads are joined.";
    }

    Status prepare() {
        RETURN_IF_ERROR(Thread::create(
                "SchemaTableSpillManager", "fetch_thread", [this]() { this->fetch_(); },
                &threads_.emplace_back()));
        RETURN_IF_ERROR(Thread::create(
                "SchemaTableSpillManager", "spill_thread", [this]() { this->spill_(); },
                &threads_.emplace_back()));
        RETURN_IF_ERROR(Thread::create(
                "SchemaTableSpillManager", "clear_thread", [this]() { this->clear_(); },
                &threads_.emplace_back()));
    }

    void register_spill_stream(TSchemaTableType::type type, size_t fetch_interval_ms, size_t ttl_s,
                               std::function<std::shared_ptr<vectorized::Block>()> fetch_function) {
        fetch_tasks_.insert(std::make_pair(
                type, SchemaTableFetchTask {fetch_interval_ms, ttl_s, std::move(fetch_function)}));
    }

    void unregister_spill_stream(TSchemaTableType::type type) { fetch_tasks_.erase(type); }

    void put_block(TSchemaTableType::type type, const std::string& datetime,
                   const std::shared_ptr<vectorized::Block>& block) {
        schema_table_time_slice_map_[type].emplace_back(datetime, block);
    }

    Status get_time_slices(TSchemaTableType::type type,
                           const vectorized::VExprContextSPtrs& expr_ctxs,
                           std::vector<std::string>& time_slice_names) {
        DCHECK(schema_table_time_slice_map_.find(type) != schema_table_time_slice_map_.end());

        // 1. get datetime expr.
        vectorized::VExprContextSPtrs effective_expr_ctxs;
        for (auto expr_ctx : expr_ctxs) {
            if (expr_ctx->root()->get_child_names() == "datetime") {
                effective_expr_ctxs.push_back(expr_ctx);
                break;
            }
        }
        if (effective_expr_ctxs.empty()) {
            return Status::OK();
        }

        vectorized::Block time_slices_block;
        // 2. list time slices.
        list_schema_time_slices_(type, &time_slices_block);

        // 3. filter time slices
        RETURN_IF_ERROR(filter_schema_time_slices_(expr_ctxs, &time_slices_block));

        // 4. extract time slices name
        // time_slices_block -> time_slice_names
    }

    bool get_time_slice_in_stream(TSchemaTableType::type type, std::string time_slice_name,
                                  vectorized::Block* output) {
        for (auto& time_slice : schema_table_time_slice_map_[type]) {
            if (time_slice.date_ == time_slice_name) {
                auto block = time_slice.block_;
                if (block->rows() != 0) {
                    output->merge(block);
                    return true;
                }
                return false;
            }
        }
    }

    Status spill_all() {
        // spill all block.
        // lock
        for (auto& [type, time_slices] : schema_table_time_slice_map_) {
            for (auto& time_slice : time_slices) {
                auto block = time_slice.block_;
                if (block->rows() == 0) {
                    continue;
                }
                size_t written_bytes = 0;
                vectorized::SpillWriterUPtr writer = std::make_unique<vectorized::SpillWriter>(
                        state_->get_query_ctx()->resource_ctx(), profile_, stream_id_, batch_rows_,
                        data_dir_, spill_dir_);
                RETURN_IF_ERROR(writer->open());
                RETURN_IF_ERROR(writer->write(state_, *block, written_bytes));
                total_written_bytes_ = writer->get_written_bytes();
            }
        }
        return Status::OK();
    }

private:
    void list_schema_time_slices_(TSchemaTableType::type type, vectorized::Block* block) {
        // 1. list spilled files.
        std::vector<std::string> filenames;
        std::string absolute_path = "/" + schema_table_spill_path(type);
        std::filesystem::list_dir(absolute_path, filenames);

        // 2. add in stream time slice.
        for (auto& time_slice : schema_table_time_slice_map_[type]) {
            filenames.push_back(time_slice.date_);
        }

        // 2. insert block, only one column of type DATETIMEV2, insert all filenames.
        auto data_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false);
        block->insert(vectorized::ColumnWithTypeAndName(data_type->create_column(), data_type,
                                                        "datetime"));
        block->reserve(filenames.size());
        vectorized::MutableColumnPtr mutable_col_ptr;
        mutable_col_ptr = std::move(block->get_by_position(0).column).assume_mutable();
        auto* nullable_column =
                reinterpret_cast<vectorized::ColumnNullable*>(mutable_col_ptr.get());
        vectorized::IColumn* col_ptr = &nullable_column->get_nested_column();
        for (auto filename : filenames) {
            std::vector<void*> datas(1);
            VecDateTimeValue src[1];
            src[0].from_date_str(filename.data(), filename.size());
            datas[0] = src;
            auto* data = datas[0];
            reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_data(
                    reinterpret_cast<char*>(data), 0);
        }
    }

    Status filter_schema_time_slices_(const vectorized::VExprContextSPtrs& expr_ctxs,
                                      vectorized::Block* block) {
        DCHECK(!expr_ctxs.empty());
        if (block->rows() == 0) {
            return Status::OK();
        }

        int prev_columns = block->columns();
        vectorized::IColumn::Filter filter;
        std::vector<ColumnId> columns_to_filter;
        columns_to_filter.push_back(0);
        RETURN_IF_ERROR(vectorized::VExprContext::execute_conjuncts_and_filter_block(
                expr_ctxs, block, columns_to_filter, prev_columns, filter));

        return Status::OK();
    }

    void fetch_() {
        do {
            for (auto& [type, task] : fetch_tasks_) {
                if (task.last_fetch_interval_ms_ > 0) {
                    task.last_fetch_interval_ms_ -= config::schema_table_fetch_interval_ms;
                    continue;
                }
                task.last_fetch_interval_ms_ = task.fetch_interval_ms_;
                auto block = task.fetch_function_();
                if (block) {
                    put_block(type, ToStringFromUnixMillis(UnixMillis()), block);
                }
            }
        } while (!stop_threads_latch_.wait_for(
                std::chrono::milliseconds(config::schema_table_fetch_interval_ms)));
    }

    void spill_() {
        do {
            auto st = spill_all();
            if (!st.ok()) {
                LOG(WARNING) << "spill schema table failed, err: " << st.get_error_msg();
            }
        } while (!stop_threads_latch_.wait_for(
                std::chrono::milliseconds(config::schema_table_spill_interval_ms)));
    }

    void clear_() {
        do {
            for (auto& [type, task] : fetch_tasks_) {
                if (task.ttl_s_ == 0) {
                    continue;
                }
                auto now = UnixMillis();
                std::vector<std::string> filenames;
                std::string absolute_path = "/" + schema_table_spill_path(type);
                std::filesystem::list_dir(absolute_path, filenames);
                for (auto& filename : filenames) {
                    if (now - task.ttl_s_ * 1000 >
                        timestamp_from_datetime(filename).to_olap_datetime()) {
                        std::string file_path = absolute_path + "/" + filename;
                        if (std::filesystem::exists(file_path)) {
                            std::filesystem::remove(file_path);
                        }
                    }
                }
            }
        } while (!stop_threads_latch_.wait_for(
                std::chrono::milliseconds(config::schema_table_spill_interval_ms)));
    }

    std::unordered_map<TSchemaTableType::type, std::vector<SchemaTableTimeSlice>>
            schema_table_time_slice_map_;
    std::unordered_map<TSchemaTableType::type, SchemaTableFetchTask> fetch_tasks_;
    std::vector<scoped_refptr<Thread>> threads_;
    CountDownLatch stop_threads_latch_;

    RuntimeState* state_ = nullptr;
    int64_t stream_id_;
    vectorized::SpillDataDir* data_dir_ = nullptr;
    std::string spill_dir_;
    size_t batch_rows_;
    int64_t total_written_bytes_ = 0;
};

#include "common/compile_check_end.h"
} // namespace doris
