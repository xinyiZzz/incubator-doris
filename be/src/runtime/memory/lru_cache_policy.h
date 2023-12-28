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

#include <fmt/format.h>

#include "olap/lru_cache.h"
#include "runtime/memory/cache_policy.h"
#include "util/time.h"

namespace doris {

// Base of the lru cache value.
struct LRUCacheValueBase {
    // Save the last visit time of this cache entry.
    // Use atomic because it may be modified by multi threads.
    std::atomic<int64_t> last_visit_time = 0;
    size_t size = 0;
};

// Base of lru cache, allow prune stale entry and prune all entry.
class LRUCachePolicy : public CachePolicy {
public:
    LRUCachePolicy(CacheType type, uint32_t stale_sweep_time_s, bool enable_prune = true)
            : CachePolicy(type, stale_sweep_time_s, enable_prune) {};
    LRUCachePolicy(CacheType type, size_t capacity, LRUCacheType lru_cache_type,
                   uint32_t stale_sweep_time_s, uint32_t num_shards = DEFAULT_LRU_CACHE_NUM_SHARDS,
                   bool enable_prune = true)
            : CachePolicy(type, stale_sweep_time_s, enable_prune) {
        init(capacity, lru_cache_type, num_shards);
    }

    bool prepare(size_t capacity, uint32_t num_shards) {
        CHECK(!_is_init);
        if (capacity < num_shards) {
            LOG(INFO) << fmt::format(
                    "{} lru cache capacity({} B) less than num_shards({}), init failed, will be "
                    "disabled.",
                    type_string(type()), capacity, num_shards);
            _enable_prune = false;
            return false;
        }
        _is_init = true;
        return true;
    }

    void init(size_t capacity, LRUCacheType lru_cache_type, uint32_t num_shards) {
        if (prepare(capacity, num_shards)) {
            _cache = std::unique_ptr<ShardedLRUCache>(
                    new ShardedLRUCache(type_string(type()), capacity, lru_cache_type, num_shards,
                                        DEFAULT_LRU_CACHE_ELEMENT_COUNT_CAPACITY));
        }
    }
    void init(size_t capacity, LRUCacheType lru_cache_type, uint32_t num_shards,
              uint32_t element_count_capacity) {
        if (prepare(capacity, num_shards)) {
            _cache = std::unique_ptr<ShardedLRUCache>(
                    new ShardedLRUCache(type_string(type()), capacity, lru_cache_type, num_shards,
                                        element_count_capacity));
        }
    }
    void init(size_t capacity, LRUCacheType lru_cache_type, uint32_t num_shards,
              CacheValueTimeExtractor cache_value_time_extractor, bool cache_value_check_timestamp,
              uint32_t element_count_capacity) {
        if (prepare(capacity, num_shards)) {
            _cache = std::unique_ptr<ShardedLRUCache>(
                    new ShardedLRUCache(type_string(type()), capacity, lru_cache_type, num_shards,
                                        cache_value_time_extractor, cache_value_check_timestamp,
                                        element_count_capacity));
        }
    }

    ~LRUCachePolicy() override = default;

    // Try to prune the cache if expired.
    void prune_stale() override {
        if (_stale_sweep_time_s <= 0 && !_is_init) {
            return;
        }
        if (_cache->mem_consumption() > CACHE_MIN_FREE_SIZE) {
            COUNTER_SET(_cost_timer, (int64_t)0);
            SCOPED_TIMER(_cost_timer);
            const int64_t curtime = UnixMillis();
            int64_t byte_size = 0L;
            auto pred = [this, curtime, &byte_size](const void* value) -> bool {
                LRUCacheValueBase* cache_value = (LRUCacheValueBase*)value;
                if ((cache_value->last_visit_time + _stale_sweep_time_s * 1000) < curtime) {
                    byte_size += cache_value->size;
                    return true;
                }
                return false;
            };

            // Prune cache in lazy mode to save cpu and minimize the time holding write lock
            COUNTER_SET(_freed_entrys_counter, _cache->prune_if(pred, true));
            COUNTER_SET(_freed_memory_counter, byte_size);
            COUNTER_UPDATE(_prune_stale_number_counter, 1);
            LOG(INFO) << fmt::format("{} prune stale {} entries, {} bytes, {} times prune",
                                     type_string(_type), _freed_entrys_counter->value(),
                                     _freed_memory_counter->value(),
                                     _prune_stale_number_counter->value());
        }
    }

    void prune_all(bool clear) override {
        if (!_is_init) {
            return;
        }
        if ((clear && _cache->mem_consumption() != 0) ||
            _cache->mem_consumption() > CACHE_MIN_FREE_SIZE) {
            COUNTER_SET(_cost_timer, (int64_t)0);
            SCOPED_TIMER(_cost_timer);
            auto size = _cache->mem_consumption();
            COUNTER_SET(_freed_entrys_counter, _cache->prune());
            COUNTER_SET(_freed_memory_counter, size);
            COUNTER_UPDATE(_prune_all_number_counter, 1);
            LOG(INFO) << fmt::format(
                    "{} prune all {} entries, {} bytes, {} times prune, is clear: {}",
                    type_string(_type), _freed_entrys_counter->value(),
                    _freed_memory_counter->value(), _prune_stale_number_counter->value(), clear);
        }
    }

    // if init failed, not expect to using Cache, if forced to use, cache() will return dummy lru cache,
    // compatible with ShardedLRUCache usage, but will not actually cache.
    bool is_init() const { return _is_init; }

    Cache* cache() const {
        if (!_is_init) {
            return ExecEnv::GetInstance()->get_dummy_lru_cache();
        }
        return _cache.get();
    }

    void reset() { _cache.reset(); }

private:
    std::unique_ptr<Cache> _cache;
    bool _is_init = false;
};

} // namespace doris
