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

#ifndef DORIS_BE_SRC_OLAP_TASK_ENGINE_STORAGE_MIGRATION_TASK_V2_H
#define DORIS_BE_SRC_OLAP_TASK_ENGINE_STORAGE_MIGRATION_TASK_V2_H

#include "gen_cpp/AgentService_types.h"
#include "olap/olap_define.h"
#include "olap/task/engine_task.h"

namespace doris {

// base class for storage engine
// add "Engine" as task prefix to prevent duplicate name with agent task
class EngineStorageMigrationTaskV2 : public EngineTask {
public:
    virtual Status execute();

public:
    EngineStorageMigrationTaskV2(const TStorageMigrationReqV2& request);
    ~EngineStorageMigrationTaskV2() {}

private:
    const TStorageMigrationReqV2& _storage_migration_req;

    std::unique_ptr<MemTrackerLimiter> _mem_tracker;
}; // EngineTask

} // namespace doris
#endif //DORIS_BE_SRC_OLAP_TASK_ENGINE_STORAGE_MIGRATION_TASK_V2_H