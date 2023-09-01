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

#include <memory>

#include "arrow/record_batch.h"

namespace doris {
namespace flight {

struct DorisStatement {
public:
    TUniqueId query_id;
    std::string sql;

    DorisStatement(const TUniqueId& query_id_, const std::string& sql_)
            : query_id(query_id_), sql(sql_) {}
};

class DorisStatementBatchReader : public arrow::RecordBatchReader {
public:
    static arrow::Result<std::shared_ptr<DorisStatementBatchReader>> Create(
            const std::shared_ptr<DorisStatement>& statement);

    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override;

    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override;

private:
    std::shared_ptr<DorisStatement> statement_;
    std::shared_ptr<arrow::Schema> schema_;

    DorisStatementBatchReader(std::shared_ptr<DorisStatement> statement,
                              std::shared_ptr<arrow::Schema> schema);
};

} // namespace flight
} // namespace doris
