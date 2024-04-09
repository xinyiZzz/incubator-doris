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

#include "exec/tablet_info.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Partitions_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/descriptors.pb.h>
#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <tuple>

#include "common/exception.h"
#include "common/status.h"
#include "olap/tablet_schema.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/large_int_value.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/primitive_type.h"
#include "runtime/raw_value.h"
#include "runtime/types.h"
#include "util/hash_util.hpp"
#include "util/string_parser.hpp"
#include "util/string_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/exprs/vliteral.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

void OlapTableIndexSchema::to_protobuf(POlapTableIndexSchema* pindex) const {
    pindex->set_id(index_id);
    pindex->set_schema_hash(schema_hash);
    for (auto slot : slots) {
        pindex->add_columns(slot->col_name());
    }
    for (auto column : columns) {
        column->to_schema_pb(pindex->add_columns_desc());
    }
    for (auto index : indexes) {
        index->to_schema_pb(pindex->add_indexes_desc());
    }
}

bool VOlapTablePartKeyComparator::operator()(const BlockRowWithIndicator lhs,
                                             const BlockRowWithIndicator rhs) const {
    vectorized::Block* l_block = std::get<0>(lhs);
    vectorized::Block* r_block = std::get<0>(rhs);
    int32_t l_row = std::get<1>(lhs);
    int32_t r_row = std::get<1>(rhs);
    bool l_use_new = std::get<2>(lhs);
    bool r_use_new = std::get<2>(rhs);

    VLOG_TRACE << '\n' << l_block->dump_data() << '\n' << r_block->dump_data();

    if (l_row == -1) {
        return false;
    } else if (r_row == -1) {
        return true;
    }

    if (_param_locs.empty()) { // no transform, use origin column
        for (auto slot_loc : _slot_locs) {
            auto res = l_block->get_by_position(slot_loc).column->compare_at(
                    l_row, r_row, *r_block->get_by_position(slot_loc).column, -1);
            if (res != 0) {
                return res < 0;
            }
        }
    } else { // use transformed column to compare
        DCHECK(_slot_locs.size() == _param_locs.size())
                << _slot_locs.size() << ' ' << _param_locs.size();

        const std::vector<uint16_t>* l_index = l_use_new ? &_param_locs : &_slot_locs;
        const std::vector<uint16_t>* r_index = r_use_new ? &_param_locs : &_slot_locs;

        for (int i = 0; i < _slot_locs.size(); i++) {
            vectorized::ColumnPtr l_col = l_block->get_by_position((*l_index)[i]).column;
            vectorized::ColumnPtr r_col = r_block->get_by_position((*r_index)[i]).column;
            //TODO: when we support any function for transform, maybe the best way is refactor all doris' functions to its essential nullable mode.
            if (auto* nullable =
                        vectorized::check_and_get_column<vectorized::ColumnNullable>(l_col)) {
                l_col = nullable->get_nested_column_ptr();
            }
            if (auto* nullable =
                        vectorized::check_and_get_column<vectorized::ColumnNullable>(r_col)) {
                r_col = nullable->get_nested_column_ptr();
            }

            auto res = l_col->compare_at(l_row, r_row, *r_col, -1);
            if (res != 0) {
                return res < 0;
            }
        }
    }

    // equal, return false
    return false;
}

Status OlapTableSchemaParam::init(const POlapTableSchemaParam& pschema) {
    _db_id = pschema.db_id();
    _table_id = pschema.table_id();
    _version = pschema.version();
    _is_partial_update = pschema.partial_update();
    _is_strict_mode = pschema.is_strict_mode();
    if (_is_partial_update) {
        _auto_increment_column = pschema.auto_increment_column();
    }

    for (auto& col : pschema.partial_update_input_columns()) {
        _partial_update_input_columns.insert(col);
    }
    std::unordered_map<std::pair<std::string, FieldType>, SlotDescriptor*> slots_map;
    _tuple_desc = _obj_pool.add(new TupleDescriptor(pschema.tuple_desc()));

    for (auto& p_slot_desc : pschema.slot_descs()) {
        auto slot_desc = _obj_pool.add(new SlotDescriptor(p_slot_desc));
        _tuple_desc->add_slot(slot_desc);
        string data_type;
        EnumToString(TPrimitiveType, to_thrift(slot_desc->col_type()), data_type);
        slots_map.emplace(std::make_pair(to_lower(slot_desc->col_name()),
                                         TabletColumn::get_field_type_by_string(data_type)),
                          slot_desc);
    }

    for (auto& p_index : pschema.indexes()) {
        auto index = _obj_pool.add(new OlapTableIndexSchema());
        index->index_id = p_index.id();
        index->schema_hash = p_index.schema_hash();
        for (auto& pcolumn_desc : p_index.columns_desc()) {
            if (!_is_partial_update ||
                _partial_update_input_columns.count(pcolumn_desc.name()) > 0) {
                auto it = slots_map.find(std::make_pair(
                        to_lower(pcolumn_desc.name()),
                        TabletColumn::get_field_type_by_string(pcolumn_desc.type())));
                if (it == std::end(slots_map)) {
                    return Status::InternalError("unknown index column, column={}, type={}",
                                                 pcolumn_desc.name(), pcolumn_desc.type());
                }
                index->slots.emplace_back(it->second);
            }
            TabletColumn* tc = _obj_pool.add(new TabletColumn());
            tc->init_from_pb(pcolumn_desc);
            index->columns.emplace_back(tc);
        }
        for (auto& pindex_desc : p_index.indexes_desc()) {
            TabletIndex* ti = _obj_pool.add(new TabletIndex());
            ti->init_from_pb(pindex_desc);
            index->indexes.emplace_back(ti);
        }
        _indexes.emplace_back(index);
    }

    std::sort(_indexes.begin(), _indexes.end(),
              [](const OlapTableIndexSchema* lhs, const OlapTableIndexSchema* rhs) {
                  return lhs->index_id < rhs->index_id;
              });
    return Status::OK();
}

Status OlapTableSchemaParam::init(const TOlapTableSchemaParam& tschema) {
    _db_id = tschema.db_id;
    _table_id = tschema.table_id;
    _version = tschema.version;
    _is_partial_update = tschema.is_partial_update;
    if (tschema.__isset.is_strict_mode) {
        _is_strict_mode = tschema.is_strict_mode;
    }
    if (_is_partial_update) {
        _auto_increment_column = tschema.auto_increment_column;
    }

    for (auto& tcolumn : tschema.partial_update_input_columns) {
        _partial_update_input_columns.insert(tcolumn);
    }
    std::unordered_map<std::pair<std::string, PrimitiveType>, SlotDescriptor*> slots_map;
    _tuple_desc = _obj_pool.add(new TupleDescriptor(tschema.tuple_desc));
    for (auto& t_slot_desc : tschema.slot_descs) {
        auto slot_desc = _obj_pool.add(new SlotDescriptor(t_slot_desc));
        _tuple_desc->add_slot(slot_desc);
        slots_map.emplace(std::make_pair(to_lower(slot_desc->col_name()), slot_desc->col_type()),
                          slot_desc);
    }

    for (auto& t_index : tschema.indexes) {
        std::unordered_map<std::string, int32_t> index_slots_map;
        auto index = _obj_pool.add(new OlapTableIndexSchema());
        index->index_id = t_index.id;
        index->schema_hash = t_index.schema_hash;
        for (auto& tcolumn_desc : t_index.columns_desc) {
            if (!_is_partial_update ||
                _partial_update_input_columns.count(tcolumn_desc.column_name) > 0) {
                auto it = slots_map.find(
                        std::make_pair(to_lower(tcolumn_desc.column_name),
                                       thrift_to_type(tcolumn_desc.column_type.type)));
                if (it == slots_map.end()) {
                    return Status::InternalError("unknown index column, column={}, type={}",
                                                 tcolumn_desc.column_name,
                                                 tcolumn_desc.column_type.type);
                }
                index->slots.emplace_back(it->second);
            }
            index_slots_map.emplace(to_lower(tcolumn_desc.column_name), tcolumn_desc.col_unique_id);
            TabletColumn* tc = _obj_pool.add(new TabletColumn());
            tc->init_from_thrift(tcolumn_desc);
            index->columns.emplace_back(tc);
        }
        if (t_index.__isset.indexes_desc) {
            for (auto& tindex_desc : t_index.indexes_desc) {
                std::vector<int32_t> column_unique_ids(tindex_desc.columns.size());
                for (size_t i = 0; i < tindex_desc.columns.size(); i++) {
                    auto it = index_slots_map.find(to_lower(tindex_desc.columns[i]));
                    if (it != index_slots_map.end()) {
                        column_unique_ids[i] = it->second;
                    }
                }
                TabletIndex* ti = _obj_pool.add(new TabletIndex());
                ti->init_from_thrift(tindex_desc, column_unique_ids);
                index->indexes.emplace_back(ti);
            }
        }
        if (t_index.__isset.where_clause) {
            RETURN_IF_ERROR(
                    vectorized::VExpr::create_expr_tree(t_index.where_clause, index->where_clause));
        }
        _indexes.emplace_back(index);
    }

    std::sort(_indexes.begin(), _indexes.end(),
              [](const OlapTableIndexSchema* lhs, const OlapTableIndexSchema* rhs) {
                  return lhs->index_id < rhs->index_id;
              });
    return Status::OK();
}

void OlapTableSchemaParam::to_protobuf(POlapTableSchemaParam* pschema) const {
    pschema->set_db_id(_db_id);
    pschema->set_table_id(_table_id);
    pschema->set_version(_version);
    pschema->set_partial_update(_is_partial_update);
    pschema->set_is_strict_mode(_is_strict_mode);
    pschema->set_auto_increment_column(_auto_increment_column);
    for (auto col : _partial_update_input_columns) {
        *pschema->add_partial_update_input_columns() = col;
    }
    _tuple_desc->to_protobuf(pschema->mutable_tuple_desc());
    for (auto slot : _tuple_desc->slots()) {
        slot->to_protobuf(pschema->add_slot_descs());
    }
    for (auto index : _indexes) {
        index->to_protobuf(pschema->add_indexes());
    }
}

std::string OlapTableSchemaParam::debug_string() const {
    std::stringstream ss;
    ss << "tuple_desc=" << _tuple_desc->debug_string();
    return ss.str();
}

VOlapTablePartitionParam::VOlapTablePartitionParam(std::shared_ptr<OlapTableSchemaParam>& schema,
                                                   const TOlapTablePartitionParam& t_param)
        : _schema(schema),
          _t_param(t_param),
          _slots(_schema->tuple_desc()->slots()),
          _mem_tracker(std::make_unique<MemTracker>("OlapTablePartitionParam")),
          _part_type(t_param.partition_type) {
    for (auto slot : _slots) {
        _partition_block.insert(
                {slot->get_empty_mutable_column(), slot->get_data_type_ptr(), slot->col_name()});
    }

    if (t_param.__isset.enable_automatic_partition && t_param.enable_automatic_partition) {
        _is_auto_partition = true;
        auto size = t_param.partition_function_exprs.size();
        _part_func_ctx.resize(size);
        _partition_function.resize(size);
        DCHECK((t_param.partition_type == TPartitionType::RANGE_PARTITIONED && size == 1) ||
               (t_param.partition_type == TPartitionType::LIST_PARTITIONED && size >= 1))
                << "now support only 1 partition column for auto range partitions. "
                << t_param.partition_type << " " << size;
        for (int i = 0; i < size; ++i) {
            Status st = vectorized::VExpr::create_expr_tree(t_param.partition_function_exprs[i],
                                                            _part_func_ctx[i]);
            if (!st.ok()) {
                throw Exception(Status::InternalError("Partition function expr is not valid"),
                                "Partition function expr is not valid");
            }
            _partition_function[i] = _part_func_ctx[i]->root();
        }
    }
}

VOlapTablePartitionParam::~VOlapTablePartitionParam() {
    _mem_tracker->release(_mem_usage);
}

Status VOlapTablePartitionParam::init() {
    std::vector<std::string> slot_column_names;
    for (auto slot_desc : _schema->tuple_desc()->slots()) {
        slot_column_names.emplace_back(slot_desc->col_name());
    }

    auto find_slot_locs = [&slot_column_names](const std::string& slot_name,
                                               std::vector<uint16_t>& locs,
                                               const std::string& column_type) {
        auto it = std::find(slot_column_names.begin(), slot_column_names.end(), slot_name);
        if (it == slot_column_names.end()) {
            return Status::InternalError("{} column not found, column ={}", column_type, slot_name);
        }
        locs.emplace_back(it - slot_column_names.begin());
        return Status::OK();
    };

    if (_t_param.__isset.partition_columns) {
        for (auto& part_col : _t_param.partition_columns) {
            RETURN_IF_ERROR(find_slot_locs(part_col, _partition_slot_locs, "partition"));
        }
    }

    _partitions_map = std::make_unique<
            std::map<BlockRowWithIndicator, VOlapTablePartition*, VOlapTablePartKeyComparator>>(
            VOlapTablePartKeyComparator(_partition_slot_locs, _transformed_slot_locs));
    if (_t_param.__isset.distributed_columns) {
        for (auto& col : _t_param.distributed_columns) {
            RETURN_IF_ERROR(find_slot_locs(col, _distributed_slot_locs, "distributed"));
        }
    }

    // for both auto/non-auto partition table.
    _is_in_partition = _part_type == TPartitionType::type::LIST_PARTITIONED;

    // initial partitions
    for (const auto& t_part : _t_param.partitions) {
        VOlapTablePartition* part = nullptr;
        RETURN_IF_ERROR(generate_partition_from(t_part, part));
        _partitions.emplace_back(part);
        if (_is_in_partition) {
            for (auto& in_key : part->in_keys) {
                _partitions_map->emplace(std::tuple {in_key.first, in_key.second, false}, part);
            }
        } else {
            _partitions_map->emplace(std::tuple {part->end_key.first, part->end_key.second, false},
                                     part);
        }
    }

    _mem_usage = _partition_block.allocated_bytes();
    _mem_tracker->consume(_mem_usage);
    return Status::OK();
}

bool VOlapTablePartitionParam::_part_contains(VOlapTablePartition* part,
                                              BlockRowWithIndicator key) const {
    // start_key.second == -1 means only single partition
    VOlapTablePartKeyComparator comparator(_partition_slot_locs, _transformed_slot_locs);
    return part->start_key.second == -1 ||
           !comparator(key, std::tuple {part->start_key.first, part->start_key.second, false});
}

Status VOlapTablePartitionParam::_create_partition_keys(const std::vector<TExprNode>& t_exprs,
                                                        BlockRow* part_key) {
    for (int i = 0; i < t_exprs.size(); i++) {
        RETURN_IF_ERROR(_create_partition_key(t_exprs[i], part_key, _partition_slot_locs[i]));
    }
    return Status::OK();
}

Status VOlapTablePartitionParam::generate_partition_from(const TOlapTablePartition& t_part,
                                                         VOlapTablePartition*& part_result) {
    DCHECK(part_result == nullptr);
    part_result = _obj_pool.add(new VOlapTablePartition(&_partition_block));
    part_result->id = t_part.id;
    part_result->is_mutable = t_part.is_mutable;
    // only load_to_single_tablet = true will set load_tablet_idx
    if (t_part.__isset.load_tablet_idx) {
        part_result->load_tablet_idx = t_part.load_tablet_idx;
    }

    if (!_is_in_partition) {
        if (t_part.__isset.start_keys) {
            RETURN_IF_ERROR(_create_partition_keys(t_part.start_keys, &part_result->start_key));
        }

        if (t_part.__isset.end_keys) {
            RETURN_IF_ERROR(_create_partition_keys(t_part.end_keys, &part_result->end_key));
        }
    } else {
        for (const auto& keys : t_part.in_keys) {
            RETURN_IF_ERROR(_create_partition_keys(
                    keys, &part_result->in_keys.emplace_back(&_partition_block, -1)));
        }
        if (t_part.__isset.is_default_partition && t_part.is_default_partition &&
            _default_partition == nullptr) {
            _default_partition = part_result;
        }
    }

    part_result->num_buckets = t_part.num_buckets;
    auto num_indexes = _schema->indexes().size();
    if (t_part.indexes.size() != num_indexes) {
        return Status::InternalError(
                "number of partition's index is not equal with schema's"
                ", num_part_indexes={}, num_schema_indexes={}",
                t_part.indexes.size(), num_indexes);
    }
    part_result->indexes = t_part.indexes;
    std::sort(part_result->indexes.begin(), part_result->indexes.end(),
              [](const OlapTableIndexTablets& lhs, const OlapTableIndexTablets& rhs) {
                  return lhs.index_id < rhs.index_id;
              });
    // check index
    for (int j = 0; j < num_indexes; ++j) {
        if (part_result->indexes[j].index_id != _schema->indexes()[j]->index_id) {
            std::stringstream ss;
            ss << "partition's index is not equal with schema's"
               << ", part_index=" << part_result->indexes[j].index_id
               << ", schema_index=" << _schema->indexes()[j]->index_id;
            return Status::InternalError(
                    "partition's index is not equal with schema's"
                    ", part_index={}, schema_index={}",
                    part_result->indexes[j].index_id, _schema->indexes()[j]->index_id);
        }
    }
    return Status::OK();
}

Status VOlapTablePartitionParam::_create_partition_key(const TExprNode& t_expr, BlockRow* part_key,
                                                       uint16_t pos) {
    auto column = std::move(*part_key->first->get_by_position(pos).column).mutate();
    switch (t_expr.node_type) {
    case TExprNodeType::DATE_LITERAL: {
        if (TypeDescriptor::from_thrift(t_expr.type).is_date_v2_type()) {
            DateV2Value<DateV2ValueType> dt;
            if (!dt.from_date_str(t_expr.date_literal.value.c_str(),
                                  t_expr.date_literal.value.size())) {
                std::stringstream ss;
                ss << "invalid date literal in partition column, date=" << t_expr.date_literal;
                return Status::InternalError(ss.str());
            }
            column->insert_data(reinterpret_cast<const char*>(&dt), 0);
        } else if (TypeDescriptor::from_thrift(t_expr.type).is_datetime_v2_type()) {
            DateV2Value<DateTimeV2ValueType> dt;
            const int32_t scale =
                    t_expr.type.types.empty() ? -1 : t_expr.type.types.front().scalar_type.scale;
            if (!dt.from_date_str(t_expr.date_literal.value.c_str(),
                                  t_expr.date_literal.value.size(), scale)) {
                std::stringstream ss;
                ss << "invalid date literal in partition column, date=" << t_expr.date_literal;
                return Status::InternalError(ss.str());
            }
            column->insert_data(reinterpret_cast<const char*>(&dt), 0);
        } else {
            VecDateTimeValue dt;
            if (!dt.from_date_str(t_expr.date_literal.value.c_str(),
                                  t_expr.date_literal.value.size())) {
                std::stringstream ss;
                ss << "invalid date literal in partition column, date=" << t_expr.date_literal;
                return Status::InternalError(ss.str());
            }
            column->insert_data(reinterpret_cast<const char*>(&dt), 0);
        }
        break;
    }
    case TExprNodeType::INT_LITERAL: {
        switch (t_expr.type.types[0].scalar_type.type) {
        case TPrimitiveType::TINYINT: {
            int8_t value = t_expr.int_literal.value;
            column->insert_data(reinterpret_cast<const char*>(&value), 0);
            break;
        }
        case TPrimitiveType::SMALLINT: {
            int16_t value = t_expr.int_literal.value;
            column->insert_data(reinterpret_cast<const char*>(&value), 0);
            break;
        }
        case TPrimitiveType::INT: {
            int32_t value = t_expr.int_literal.value;
            column->insert_data(reinterpret_cast<const char*>(&value), 0);
            break;
        }
        default:
            int64_t value = t_expr.int_literal.value;
            column->insert_data(reinterpret_cast<const char*>(&value), 0);
        }
        break;
    }
    case TExprNodeType::LARGE_INT_LITERAL: {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        __int128 value = StringParser::string_to_int<__int128>(
                t_expr.large_int_literal.value.c_str(), t_expr.large_int_literal.value.size(),
                &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            value = MAX_INT128;
        }
        column->insert_data(reinterpret_cast<const char*>(&value), 0);
        break;
    }
    case TExprNodeType::STRING_LITERAL: {
        int len = t_expr.string_literal.value.size();
        const char* str_val = t_expr.string_literal.value.c_str();
        column->insert_data(str_val, len);
        break;
    }
    case TExprNodeType::BOOL_LITERAL: {
        column->insert_data(reinterpret_cast<const char*>(&t_expr.bool_literal.value), 0);
        break;
    }
    case TExprNodeType::NULL_LITERAL: {
        // insert a null literal
        column->insert_data(nullptr, 0);
        break;
    }
    default: {
        return Status::InternalError("unsupported partition column node type, type={}",
                                     t_expr.node_type);
    }
    }
    part_key->second = column->size() - 1;
    return Status::OK();
}

Status VOlapTablePartitionParam::add_partitions(
        const std::vector<TOlapTablePartition>& partitions) {
    for (const auto& t_part : partitions) {
        auto part = _obj_pool.add(new VOlapTablePartition(&_partition_block));
        part->id = t_part.id;
        part->is_mutable = t_part.is_mutable;

        DCHECK(t_part.__isset.start_keys == t_part.__isset.end_keys &&
               t_part.__isset.start_keys != t_part.__isset.in_keys);
        // range partition
        if (t_part.__isset.start_keys) {
            RETURN_IF_ERROR(_create_partition_keys(t_part.start_keys, &part->start_key));
        }
        if (t_part.__isset.end_keys) {
            RETURN_IF_ERROR(_create_partition_keys(t_part.end_keys, &part->end_key));
        }
        // list partition - we only set 1 value in 1 partition for new created ones
        if (t_part.__isset.in_keys) {
            for (const auto& keys : t_part.in_keys) {
                RETURN_IF_ERROR(_create_partition_keys(
                        keys, &part->in_keys.emplace_back(&_partition_block, -1)));
            }
            if (t_part.__isset.is_default_partition && t_part.is_default_partition) {
                _default_partition = part;
            }
        }

        part->num_buckets = t_part.num_buckets;
        auto num_indexes = _schema->indexes().size();
        if (t_part.indexes.size() != num_indexes) {
            return Status::InternalError(
                    "number of partition's index is not equal with schema's"
                    ", num_part_indexes={}, num_schema_indexes={}",
                    t_part.indexes.size(), num_indexes);
        }
        part->indexes = t_part.indexes;
        std::sort(part->indexes.begin(), part->indexes.end(),
                  [](const OlapTableIndexTablets& lhs, const OlapTableIndexTablets& rhs) {
                      return lhs.index_id < rhs.index_id;
                  });
        // check index
        for (int j = 0; j < num_indexes; ++j) {
            if (part->indexes[j].index_id != _schema->indexes()[j]->index_id) {
                std::stringstream ss;
                ss << "partition's index is not equal with schema's"
                   << ", part_index=" << part->indexes[j].index_id
                   << ", schema_index=" << _schema->indexes()[j]->index_id;
                return Status::InternalError(
                        "partition's index is not equal with schema's"
                        ", part_index={}, schema_index={}",
                        part->indexes[j].index_id, _schema->indexes()[j]->index_id);
            }
        }
        _partitions.emplace_back(part);
        // after _creating_partiton_keys
        if (_is_in_partition) {
            for (auto& in_key : part->in_keys) {
                _partitions_map->emplace(std::tuple {in_key.first, in_key.second, false}, part);
            }
        } else {
            _partitions_map->emplace(std::tuple {part->end_key.first, part->end_key.second, false},
                                     part);
        }
    }

    return Status::OK();
}

} // namespace doris
