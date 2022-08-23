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

#include "exec/exec_node.h"
#include "exec/olap_common.h"
#include "exprs/function_filter.h"
#include "exprs/runtime_filter.h"
#include "vec/exec/scan/scanner_context.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vin_predicate.h"

namespace doris::vectorized {

class VScanner;
class VSlotRef;

class VScanNode : public ExecNode {
public:
    VScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : ExecNode(pool, tnode, descs) {}
    friend class NewOlapScanner;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    virtual void set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {}

    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override {
        return Status::NotSupported("Not implement");
    }

    // Get next block.
    // If eos is true, no more data will be read and block should be empty.
    Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    Status close(RuntimeState* state) override;

    void set_no_agg_finalize() { _need_agg_finalize = false; }

    // Try append late arrived runtime filters.
    // Return num of filters which are applied already.
    Status try_append_late_arrival_runtime_filter(int* arrived_rf_num);

    // Clone current vconjunct_ctx to _vconjunct_ctx, if exists.
    Status clone_vconjunct_ctx(VExprContext** _vconjunct_ctx);

    int runtime_filter_num() const { return (int)_runtime_filter_ctxs.size(); }

    TupleId input_tuple_id() const { return _input_tuple_id; }
    TupleId output_tuple_id() const { return _output_tuple_id; }
    const TupleDescriptor* input_tuple_desc() const { return _input_tuple_desc; }
    const TupleDescriptor* output_tuple_desc() const { return _output_tuple_desc; }

protected:
    // Different data sources register different profiles by implementing this method
    virtual Status _init_profile() { return Status::OK(); }

    // Process predicates, extract the predicates in the conjuncts that can be pushed down
    // to the data source, and convert them into common expressions structure ColumnPredicate.
    // There are currently 3 types of predicates that can be pushed down to data sources:
    //
    // 1. Simple predicate, with column on left and constant on right, such as "a=1", "b in (1,2,3)" etc.
    // 2. Bloom Filter, predicate condition generated by runtime filter
    // 3. Function Filter, some data sources can accept function conditions, such as "a like 'abc%'"
    //
    // Predicates that can be fully processed by the data source will be removed from conjuncts
    virtual Status _process_conjuncts() {
        RETURN_IF_ERROR(_normalize_conjuncts());
        return Status::OK();
    }

    // Create a list of scanners.
    // The number of scanners is related to the implementation of the data source,
    // predicate conditions, and scheduling strategy.
    // So this method needs to be implemented separately by the subclass of ScanNode.
    // Finally, a set of scanners that have been prepared are returned.
    virtual Status _init_scanners(std::list<VScanner*>* scanners) { return Status::OK(); }

    //  Different data sources can implement the following 3 methods to determine whether a predicate
    //  can be pushed down to the data source.
    //  3 types:
    //      1. binary predicate
    //      2. in/not in predicate
    //      3. function predicate
    //  TODO: these interfaces should be change to become more common.
    virtual bool _should_push_down_binary_predicate(
            VectorizedFnCall* fn_call, VExprContext* expr_ctx, StringRef* constant_val,
            int* slot_ref_child, const std::function<bool(const std::string&)>& fn_checker) {
        return false;
    }

    virtual bool _should_push_down_in_predicate(VInPredicate* in_pred, VExprContext* expr_ctx,
                                                bool is_not_in) {
        return false;
    }

    virtual bool _should_push_down_function_filter(VectorizedFnCall* fn_call,
                                                   VExprContext* expr_ctx, StringVal* constant_str,
                                                   doris_udf::FunctionContext** fn_ctx) {
        return false;
    }

    // Return true if it is a key column.
    // Only predicate on key column can be pushed down.
    virtual bool _is_key_column(const std::string& col_name) { return false; }

protected:
    RuntimeState* _state;
    // For load scan node, there should be both input and output tuple descriptor.
    // For query scan node, there is only output_tuple_desc.
    TupleId _input_tuple_id = -1;
    TupleId _output_tuple_id = -1;
    const TupleDescriptor* _input_tuple_desc;
    const TupleDescriptor* _output_tuple_desc;

    // These two values are from query_options
    int _max_scan_key_num;
    int _max_pushdown_conditions_per_column;

    // For runtime filters
    struct RuntimeFilterContext {
        RuntimeFilterContext() : apply_mark(false), runtime_filter(nullptr) {}
        // set to true if this runtime filter is already applied to vconjunct_ctx_ptr
        bool apply_mark;
        IRuntimeFilter* runtime_filter;
    };
    std::vector<RuntimeFilterContext> _runtime_filter_ctxs;

    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
    // Set to true if the runtime filter is ready.
    std::vector<bool> _runtime_filter_ready_flag;
    std::mutex _rf_locks;
    std::map<int, RuntimeFilterContext*> _conjunct_id_to_runtime_filter_ctxs;
    phmap::flat_hash_set<VExpr*> _rf_vexpr_set;
    // True means all runtime filters are applied to scanners
    bool _is_all_rf_applied = true;

    // Each scan node will generates a ScannerContext to manage all Scanners.
    // See comments of ScannerContext for more details
    std::shared_ptr<ScannerContext> _scanner_ctx;
    // Save all scanner objects.
    ObjectPool _scanner_pool;

    // indicate this scan node has no more data to return
    bool _eos = false;

    // Save all bloom filter predicates which may be pushed down to data source.
    // column name -> bloom filter function
    std::vector<std::pair<std::string, std::shared_ptr<IBloomFilterFuncBase>>>
            _bloom_filters_push_down;

    // Save all function predicates which may be pushed down to data source.
    std::vector<FunctionFilter> _push_down_functions;

    // slot id -> ColumnValueRange
    // Parsed from conjunts
    phmap::flat_hash_map<int, std::pair<SlotDescriptor*, ColumnValueRangeType>>
            _slot_id_to_value_range;
    // column -> ColumnValueRange
    std::map<std::string, ColumnValueRangeType> _colname_to_value_range;

    bool _need_agg_finalize = true;

    // TODO: should be moved to olap scan node?
    std::vector<TCondition> _olap_filters;

    // Every time vconjunct_ctx_ptr is updated, the old ctx will be stored in this vector
    // so that it will be destroyed uniformly at the end of the query.
    std::vector<std::unique_ptr<VExprContext*>> _stale_vexpr_ctxs;

    // If sort info is set, push limit to each scanner;
    int64_t _limit_per_scanner = -1;

private:
    // Register and get all runtime filters at Init phase.
    Status _register_runtime_filter();
    // Get all arrived runtime filters at Open phase.
    Status _acquire_runtime_filter();
    // Append late-arrival runtime filters to the vconjunct_ctx.
    Status _append_rf_into_conjuncts(std::vector<VExpr*>& vexprs);

    Status _normalize_conjuncts();
    VExpr* _normalize_predicate(VExpr* conjunct_expr_root);
    void _eval_const_conjuncts(VExpr* vexpr, VExprContext* expr_ctx, bool* push_down);

    Status _normalize_bloom_filter(VExpr* expr, VExprContext* expr_ctx, SlotDescriptor* slot,
                                   bool* push_down);

    Status _normalize_function_filters(VExpr* expr, VExprContext* expr_ctx, SlotDescriptor* slot,
                                       bool* push_down);

    bool _is_predicate_acting_on_slot(VExpr* expr,
                                      const std::function<bool(const std::vector<VExpr*>&,
                                                               const VSlotRef**, VExpr**)>& checker,
                                      SlotDescriptor** slot_desc, ColumnValueRangeType** range);

    template <PrimitiveType T>
    Status _normalize_in_and_eq_predicate(vectorized::VExpr* expr, VExprContext* expr_ctx,
                                          SlotDescriptor* slot, ColumnValueRange<T>& range,
                                          bool* push_down);
    template <PrimitiveType T>
    Status _normalize_not_in_and_not_eq_predicate(vectorized::VExpr* expr, VExprContext* expr_ctx,
                                                  SlotDescriptor* slot, ColumnValueRange<T>& range,
                                                  bool* push_down);

    template <PrimitiveType T>
    Status _normalize_noneq_binary_predicate(vectorized::VExpr* expr, VExprContext* expr_ctx,
                                             SlotDescriptor* slot, ColumnValueRange<T>& range,
                                             bool* push_down);

    template <PrimitiveType T>
    Status _normalize_is_null_predicate(vectorized::VExpr* expr, VExprContext* expr_ctx,
                                        SlotDescriptor* slot, ColumnValueRange<T>& range,
                                        bool* push_down);

    template <bool IsFixed, PrimitiveType PrimitiveType, typename ChangeFixedValueRangeFunc>
    static Status _change_value_range(ColumnValueRange<PrimitiveType>& range, void* value,
                                      const ChangeFixedValueRangeFunc& func,
                                      const std::string& fn_name, bool cast_date_to_datetime = true,
                                      int slot_ref_child = -1);

    // Submit the scanner to the thread pool and start execution
    Status _start_scanners(const std::list<VScanner*>& scanners);
};

} // namespace doris::vectorized
