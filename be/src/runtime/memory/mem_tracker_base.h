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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/mem-tracker.h
// and modified by Doris

#pragma once

#include "util/runtime_profile.h"

namespace doris {

class MemTrackerLimiter;

/// A MemTracker tracks memory consumption; it contains an optional limit
/// and can be arranged into a tree structure such that the consumption tracked
/// by a MemTracker is also tracked by its ancestors.
///
/// We use a five-level hierarchy of mem trackers: process, pool, query, fragment
/// instance. Specific parts of the fragment (exec nodes, sinks, etc) will add a
/// fifth level when they are initialized. This function also initializes a user
/// function mem tracker (in the fifth level).
///
/// By default, memory consumption is tracked via calls to Consume()/Release(), either to
/// the tracker itself or to one of its descendents. Alternatively, a consumption metric
/// can be specified, and then the metric's value is used as the consumption rather than
/// the tally maintained by Consume() and Release(). A tcmalloc metric is used to track
/// process memory consumption, since the process memory usage may be higher than the
/// computed total memory (tcmalloc does not release deallocated memory immediately).
/// Other consumption metrics are used in trackers below the process level to account
/// for memory (such as free buffer pool buffers) that is not tracked by Consume() and
/// Release().
///
/// GcFunctions can be attached to a MemTracker in order to free up memory if the limit is
/// reached. If limit_exceeded() is called and the limit is exceeded, it will first call
/// the GcFunctions to try to free memory and recheck the limit. For example, the process
/// tracker has a GcFunction that releases any unused memory still held by tcmalloc, so
/// this will be called before the process limit is reported as exceeded. GcFunctions are
/// called in the order they are added, so expensive functions should be added last.
/// GcFunctions are called with a global lock held, so should be non-blocking and not
/// call back into MemTrackers, except to release memory.
//
/// This class is thread-safe.
class MemTrackerBase {
public:
    const std::string& label() const { return _label; }

    // Returns the memory consumed in bytes.
    int64_t consumption() const { return _consumption->current_value(); }
    int64_t peak_consumption() const { return _consumption->value(); }

    // 'label' is the label used in the usage string (log_usage())
    MemTrackerBase(const std::string& label, MemTrackerLimiter* parent, RuntimeProfile* profile);

    // this is used for creating an orphan mem tracker, or for unit test.
    // If a mem tracker has parent, it should be created by `create_tracker()`
    MemTrackerBase(const std::string& label = std::string());

    MemTrackerLimiter* parent() const { return _parent; }
    int64_t id() { return _id; }
    bool is_limited() { return _is_limited; }
    bool is_observed() { return _is_observed; }
    void set_is_limited() { _is_limited = true; }
    void set_is_observed() { _is_observed = true; }

    static const std::string COUNTER_NAME;

protected:
    //
    std::string _label;

    //
    int64_t _id;

    // in bytes
    std::shared_ptr<RuntimeProfile::HighWaterMarkCounter> _consumption;

    //
    bool _is_limited = false;

    //
    bool _is_observed = false;

    MemTrackerLimiter* _parent; // The parent of this tracker.
};

} // namespace doris
