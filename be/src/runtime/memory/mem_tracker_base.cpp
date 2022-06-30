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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/mem-tracker.cpp
// and modified by Doris

#include "runtime/memory/mem_tracker_base.h"
#include "util/time.h"

namespace doris {

const std::string MemTrackerBase::COUNTER_NAME = "PeakMemoryUsage";

MemTrackerBase::MemTrackerBase(const std::string& label, MemTrackerLimiter* parent,
                       RuntimeProfile* profile)
        : _label(label),
          // Not 100% sure the id is unique. This is generated because it is faster than converting to int after hash.
          _id((GetCurrentTimeMicros() % 1000000) * 100 + _label.length()),
          _parent(parent) {
    if (profile == nullptr) {
        _consumption = std::make_shared<RuntimeProfile::HighWaterMarkCounter>(TUnit::BYTES);
    } else {
        _consumption = profile->AddSharedHighWaterMarkCounter(COUNTER_NAME, TUnit::BYTES);
    }
}

MemTrackerBase::MemTrackerBase(const std::string& label)
        : MemTrackerBase(label, nullptr, nullptr) {}
} // namespace doris
