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

#include "common/factory_creator.h"
#include "common/multi_version.h"
#include "runtime/workload_management/io_throttle.h"
#include "util/runtime_profile.h"

namespace doris {

class IOContext : public std::enable_shared_from_this<IOContext> {
    ENABLE_FACTORY_CREATOR(IOContext);

public:
    /*
    * 1. operate them thread-safe.
    * 2. all tasks are unified.
    * 3. should not be operated frequently, use local variables to update Counter.
    */
    struct Stats {
        Stats() {
            init_profile();
        }

        void merge(const Stats& other) const {
            scan_rows_counter_->update(other.scan_rows_counter_->value());
            scan_bytes_counter_->update(other.scan_bytes_counter_->value());
            scan_bytes_from_local_storage_counter_->update(other.scan_bytes_from_local_storage_counter_->value());
            scan_bytes_from_remote_storage_counter_->update(other.scan_bytes_from_remote_storage_counter_->value());
            returned_rows_counter_->update(other.returned_rows_counter_->value());
            shuffle_send_bytes_counter_->update(other.shuffle_send_bytes_counter_->value());
            shuffle_send_rows_counter_->update(other.shuffle_send_rows_counter_->value());
        }

        RuntimeProfile::Counter* scan_rows_counter_;
        RuntimeProfile::Counter* scan_bytes_counter_;
        RuntimeProfile::Counter* scan_bytes_from_local_storage_counter_;
        RuntimeProfile::Counter* scan_bytes_from_remote_storage_counter_;
        // number rows returned by query.
        // only set once by result sink when closing.
        RuntimeProfile::Counter* returned_rows_counter_;
        RuntimeProfile::Counter* shuffle_send_bytes_counter_;
        RuntimeProfile::Counter* shuffle_send_rows_counter_;

        RuntimeProfile* profile() { return profile_.get(); }
        void init_profile() {
            profile_ = std::make_unique<RuntimeProfile>("MemoryContext");
            scan_rows_counter_ = ADD_COUNTER(profile_, "ScanRows", TUnit::UNIT);
            scan_bytes_counter_ = ADD_COUNTER(profile_, "ScanBytes", TUnit::BYTES);
            scan_bytes_from_local_storage_counter_ =
                    ADD_COUNTER(profile_, "ScanBytesFromLocalStorage", TUnit::BYTES);
            scan_bytes_from_remote_storage_counter_ =
                    ADD_COUNTER(profile_, "ScanBytesFromRemoteStorage", TUnit::BYTES);
            returned_rows_counter_ = ADD_COUNTER(profile_, "ReturnedRows", TUnit::UNIT);
            shuffle_send_bytes_counter_ = ADD_COUNTER(profile_, "ShuffleSendBytes", TUnit::BYTES);
            shuffle_send_rows_counter_ =
                    ADD_COUNTER(profile_, "ShuffleSendRowsCounter_", TUnit::UNIT);
        }
        std::string debug_string() { return profile_->pretty_print(); }

    private:
        std::unique_ptr<RuntimeProfile> profile_;
    };

    IOContext() { 
        stats_.set(std::make_unique<Stats>());
    }
    virtual ~IOContext() = default;
    // read only
    void merge_stats() {
        std::unique_ptr<Stats> stats = std::make_unique<Stats>();
        for (auto const& st : stats_list_) {
            stats->merge(*st);
        }
        stats_.set(std::move(stats));
    }
    std::shared_ptr<Stats> stats() {
        merge_stats();
        return stats_.get();
    }
    void register_stats(std::shared_ptr<Stats> st) {
        stats_list_.push_back(st);
    }

    IOThrottle* io_throttle() {
        // TODO: get io throttle from workload group
        return nullptr;
    }

protected:
    MultiVersion<Stats> stats_; // read only
    std::vector<std::shared_ptr<Stats>> stats_list_;
};

} // namespace doris
