#include <ranges>
#include <iostream>

#include <duckdb.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_expression_get.hpp>
#include <duckdb/planner/operator/logical_aggregate.hpp>
#include <duckdb/planner/operator/logical_cteref.hpp>
#include <duckdb/planner/operator/logical_delim_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_comparison_join.hpp>
#include <duckdb/planner/operator/logical_any_join.hpp>
#include <duckdb/planner/operator/logical_materialized_cte.hpp>
#include <duckdb/planner/operator/logical_recursive_cte.hpp>
#include <duckdb/planner/operator/logical_set_operation.hpp>
#include <duckdb/planner/operator/logical_window.hpp>
#include <duckdb/planner/operator/logical_unnest.hpp>
#include <duckdb/planner/operator/logical_delete.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/planner/bound_tableref.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/common/extra_type_info.hpp>

#include <magic_enum/magic_enum.hpp>

#include "duckdb_logical_visitors.hpp"

namespace worker {

auto SampleNullabilityNode::findByName(const std::string& name) -> std::shared_ptr<SampleNullabilityNode> {
    for (auto& child_node: this->children) {
        if (child_node->name == name) {
            return child_node;
        }
    }

    return nullptr;
}

using ColumnRefNullabilityMap = std::unordered_map<duckdb::idx_t, bool>;

static auto EvaluateNullability(duckdb::JoinType rel_join_type, const JoinTypeVisitor::ConditionRels& rel_map, const ColumnNullableLookup& internal_lookup, const ColumnNullableLookup& lookup) -> bool {
    if (rel_join_type == duckdb::JoinType::OUTER) return true;

    return std::ranges::any_of(rel_map | std::views::values, [&](const auto& to) { return lookup[to].shouldNulls(); });
}

static auto getColumnRefNullabilitiesInternal(const duckdb::TableCatalogEntry& table, ZmqChannel& channel) -> ColumnRefNullabilityMap {
    auto constraints = 
        table.GetConstraints()
        | std::views::filter([](const duckdb::unique_ptr<duckdb::Constraint>& c) {
            return c->type == duckdb::ConstraintType::NOT_NULL;
        })
        | std::views::transform([](const duckdb::unique_ptr<duckdb::Constraint>& c) {
            auto& nn_constraint = c->Cast<duckdb::NotNullConstraint>();

            return std::make_pair<duckdb::idx_t, bool>(
                std::move(nn_constraint.index.index), 
                true
            );
        })
    ;
    return ColumnRefNullabilityMap(constraints.begin(), constraints.end());
}

static auto getColumnRefNullabilities(const duckdb::LogicalGet& op, ZmqChannel& channel) -> ColumnRefNullabilityMap {
    auto bind_info = op.function.get_bind_info(op.bind_data);
    
    if (! bind_info.table) {
        channel.warn(std::format("[TODO] cannot find table catalog: (index: {})", op.table_index));
        return {};
    }

    return getColumnRefNullabilitiesInternal(*bind_info.table, channel);
}

static auto getSampleNullabilitiesInternal(const duckdb::Vector& vec, const duckdb::LogicalType& ty, const std::string& name, const size_t sampling_rows, ZmqChannel& channel) -> std::shared_ptr<SampleNullabilityNode> {
    auto node = std::make_shared<SampleNullabilityNode>(
        name,
        ColumnNullableLookup::Item{ .from_field = (! duckdb::FlatVector::Validity(vec).CheckAllValid(sampling_rows)), .from_join = false }
    );
    
    switch (ty.id()) {
    case duckdb::LogicalTypeId::STRUCT:
        {
            auto& entries = duckdb::StructVector::GetEntries(vec);
            auto& child_types = ty.AuxInfo()->Cast<duckdb::StructTypeInfo>().child_types;
            node->children.reserve(child_types.size());

            for (size_t c = 0; c < entries.size(); ++c) {
                std::string child_name = child_types[c].first;
                node->children.emplace_back(getSampleNullabilitiesInternal(*entries[c], child_types[c].second, child_name, sampling_rows, channel));
            }
        }
        break;
    case duckdb::LogicalTypeId::LIST:
        {
            auto& entries = duckdb::ListVector::GetEntry(vec);
            auto& child_type = ty.AuxInfo()->Cast<duckdb::ListTypeInfo>().child_type;
            node->children.reserve(1);

            node->children.emplace_back(getSampleNullabilitiesInternal(entries, child_type, name, sampling_rows * duckdb::ListVector::GetListSize(vec), channel));
        }
        break;
    default:
        // Supporse primitive or null type
        break;
    }

    return node;
}

static auto measureSamplingRows(const duckdb::unique_ptr<duckdb::DataChunk>& chunk) -> size_t {
    std::function<size_t (const duckdb::Vector&)> measure_internal = [&](const duckdb::Vector& vec) -> size_t {
        switch (vec.GetType().id()) {
        case duckdb::LogicalTypeId::STRUCT:
            {
                size_t count = 0;
                for (auto& child_vec: duckdb::StructVector::GetEntries(vec)) {
                    count += measure_internal(*child_vec);
                }
                return count;
            }
        case duckdb::LogicalTypeId::LIST: return duckdb::ListVector::GetListSize(vec);
        default: return 1;
        }
    };

    size_t col_count = 0;

    for (auto& vec: chunk->data) {
        col_count += measure_internal(vec);
    }

    return std::min<size_t>(col_count * col_count * 2, chunk->size());
}

static auto getSampleNullabilities(const duckdb::LogicalGet& op, duckdb::Connection& conn, ZmqChannel& channel) -> std::unordered_map<std::string, std::shared_ptr<SampleNullabilityNode>> {
    std::unordered_map<std::string, std::shared_ptr<SampleNullabilityNode>> sample_cache{};
    
    if (op.projected_input.size() > 0) return sample_cache;

    auto sample = conn.TableFunction(op.function.name, op.parameters, op.named_parameters)->Execute();
    auto chunk = sample->FetchRaw();
    auto sampling_rows = measureSamplingRows(chunk);

    for (size_t c = 0; c < chunk->data.size(); ++c) {
        sample_cache[op.names[c]] = getSampleNullabilitiesInternal(chunk->data[c], op.returned_types[c], op.names[c], sampling_rows, channel);
    }

    return std::move(sample_cache);
}

auto JoinTypeVisitor::VisitOperatorGet(const duckdb::LogicalGet& op) -> void {
    if (op.function.get_bind_info) {
        // From table
        auto constraints = getColumnRefNullabilities(op, this->channel);

        auto sz = constraints.size();

        for (duckdb::idx_t c = 0; auto id: op.GetColumnIds()) {
            const ColumnNullableLookup::Column binding{
                .table_index = op.table_index, 
                .column_index = c++
            };
            
            this->join_type_lookup[binding] = ColumnNullableLookup::Item{
                .from_field = (!constraints[id]),
                .from_join = false
            };
        }
    }
    else {
        // From table function
        auto constraints = getSampleNullabilities(op, this->conn, this->channel);

        for (duckdb::idx_t c = 0; auto col_name: op.names) {
            const ColumnNullableLookup::Column binding{
                .table_index = op.table_index, 
                .column_index = c++
            };
            
            if (constraints.empty()) {
                this->join_type_lookup[binding].from_field = true;
            }
            else {
                auto &node = constraints[col_name];
                this->join_type_lookup[binding].from_field = node->nullable.from_field;
                this->sample_cache.insert(binding, node);
            }
        }      
    }
}

auto JoinTypeVisitor::VisitOperatorValuesGet(duckdb::LogicalExpressionGet& op) -> void {
    if (op.expressions.empty()) return;

    for (duckdb::idx_t c = 0; auto& expr: op.expressions[0]) {
        const ColumnNullableLookup::Column binding{
            .table_index = op.table_index, 
            .column_index = c++
        };
        
        this->join_type_lookup[binding] = ColumnNullableLookup::Item{
            .from_field = ColumnExpressionVisitor::Resolve(expr, binding, this->parent_lookup, this->sample_cache).from_field,
            .from_join = false
        };
    }
}

auto JoinTypeVisitor::VisitOperatorGroupBy(duckdb::LogicalAggregate& op) -> void {
    ColumnNullableLookup internal_join_types{};
    JoinTypeVisitor visitor(internal_join_types, this->join_type_lookup, this->sample_cache, this->conn, this->channel);

    for (auto& c: op.children) {
        visitor.VisitOperator(*c);
    }

    group_index: {
        for (duckdb::idx_t c = 0; auto& expr: op.groups) {
            ColumnNullableLookup::Column to_binding{
                .table_index = op.group_index, 
                .column_index = c, 
            };

            this->join_type_lookup[to_binding] = ColumnExpressionVisitor::Resolve(expr, to_binding, internal_join_types, this->sample_cache);
            ++c;
        }
    }
    aggregate_index: {
        for (duckdb::idx_t c = 0; auto& expr: op.expressions) {
            ColumnNullableLookup::Column to_binding{
                .table_index = op.aggregate_index, 
                .column_index = c, 
            };

            this->join_type_lookup[to_binding] = ColumnExpressionVisitor::Resolve(expr, to_binding, internal_join_types, this->sample_cache);
            ++c;
        }
    }
    groupings_index: {
        // for (duckdb::idx_t c = 0; c < op.grouping_functions.size(); ++c) {
        for (duckdb::idx_t c = 0; auto& expr: op.grouping_functions) {
            ColumnNullableLookup::Column to_binding{
                .table_index = op.groupings_index, 
                .column_index = c++, 
            };
            this->join_type_lookup[to_binding] = {.from_field = true, .from_join = false};
        }
    }
}

auto JoinTypeVisitor::VisitOperatorCteRef(duckdb::LogicalCTERef& op) -> void {
    for (duckdb::idx_t c = 0; auto& col_name: op.bound_columns) {
        {
            ColumnNullableLookup::Column from_binding{
                .table_index = op.cte_index, 
                .column_index = c, 
            };
            ColumnNullableLookup::Column to_binding{
                .table_index = op.table_index, 
                .column_index = c, 
            };
            
            this->join_type_lookup[to_binding] = this->parent_lookup[from_binding];
        }
        ++c;
    }
}

auto JoinTypeVisitor::VisitOperatorCondition(duckdb::LogicalOperator &op, duckdb::JoinType join_type, const ConditionRels& rels) -> ColumnNullableLookup {
    ColumnNullableLookup internal_join_types{};
    JoinTypeVisitor visitor(internal_join_types, this->parent_lookup, this->sample_cache, this->conn, this->channel);

    visitor.VisitOperator(op);

    return std::move(internal_join_types);
}

auto JoinTypeVisitor::VisitOperatorJoinInternal(duckdb::LogicalOperator &op, duckdb::JoinType ty_left, duckdb::JoinType ty_right, ConditionRels&& rels) -> void {
    resolve_join_type: {
        // walk in left
        for (auto& [binding, nullable]: this->VisitOperatorCondition(*op.children[0], ty_left, {})) {
            this->join_type_lookup[binding] = {
                .from_field = nullable.from_field,
                .from_join = nullable.from_join || (ty_left == duckdb::JoinType::OUTER), 
            };
        }
        auto left_count = this->join_type_lookup.size();

        // walk in right
        auto internal_join_types = this->VisitOperatorCondition(*op.children[1], ty_right, rels);
        auto join_is_null = EvaluateNullability(ty_right, rels, internal_join_types, this->join_type_lookup);

        for (auto& [binding, nullable]: internal_join_types) {
            this->join_type_lookup[binding] = {
                .from_field = nullable.from_field, 
                .from_join = join_is_null,
            };
        }
    }
}

namespace binding {
    static std::unordered_set<duckdb::JoinType> filter_joins{ 
        duckdb::JoinType::SEMI,
        duckdb::JoinType::ANTI,
        duckdb::JoinType::MARK
    };

    class ConditionBindingVisitor: public duckdb::LogicalOperatorVisitor {
    public:
        ConditionBindingVisitor(JoinTypeVisitor::ConditionRels& rels): condition_rels(rels) {}
    protected:
        auto VisitReplace(duckdb::BoundColumnRefExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
            ColumnNullableLookup::Column col{
                .table_index = expr.binding.table_index, 
                .column_index = expr.binding.column_index, 
            };
            this->condition_rels[col] = col;

            return nullptr;
        }
    private:
        JoinTypeVisitor::ConditionRels& condition_rels;
    };

    class DelimGetBindingVisitor: public duckdb::LogicalOperatorVisitor {
    public:
        DelimGetBindingVisitor(JoinTypeVisitor::ConditionRels& rels, std::vector<ColumnNullableLookup::Column>&& map_to)
            : condition_rels(rels), map_to(map_to)
        {
        }
    public:
        auto VisitOperator(duckdb::LogicalOperator &op) -> void {
            if (op.type == duckdb::LogicalOperatorType::LOGICAL_DELIM_GET) {
                auto& op_get = op.Cast<duckdb::LogicalDelimGet>();
                for (duckdb::idx_t i = 0; i < op_get.chunk_types.size(); ++i) {
                    ColumnNullableLookup::Column from{
                        .table_index = op_get.table_index, 
                        .column_index = i, 
                    };
                    this->condition_rels[from] = this->map_to[i];
                }
                return;
            }
            
            if (op.type == duckdb::LogicalOperatorType::LOGICAL_DELIM_JOIN) {
                auto& op_join = op.Cast<duckdb::LogicalJoin>();
                if (filter_joins.contains(op_join.join_type)) return;
            }
            
            duckdb::LogicalOperatorVisitor::VisitOperator(op);
        }
    private:
        JoinTypeVisitor::ConditionRels& condition_rels;
        std::vector<ColumnNullableLookup::Column> map_to;
    };
}

static auto VisitJoinCondition(duckdb::vector<duckdb::JoinCondition>& conditions) -> JoinTypeVisitor::ConditionRels {
    JoinTypeVisitor::ConditionRels rel_map{};
    condition: {
        binding::ConditionBindingVisitor visitor(rel_map);

        for (auto& c: conditions) {
            visitor.VisitExpression(&c.left);
            visitor.VisitExpression(&c.right);
        }
    }

    return std::move(rel_map);
}

static auto VisitDelimJoinCondition(duckdb::LogicalComparisonJoin& op, std::vector<ColumnNullableLookup::Column>&& map_to) -> JoinTypeVisitor::ConditionRels {
    JoinTypeVisitor::ConditionRels rel_map{};
    delim_get: {
        binding::DelimGetBindingVisitor visitor(rel_map, std::move(map_to));

        for (auto& c: op.children) {
            visitor.VisitOperator(*c);
        }
    }
    condition: {
        JoinTypeVisitor::ConditionRels rel_map2{};
        binding::ConditionBindingVisitor visitor(rel_map2);

        for (auto& c: op.conditions) {
            visitor.VisitExpression(&c.left);
            visitor.VisitExpression(&c.right);
        }

        rel_map.insert(rel_map2.begin(), rel_map2.end());
    }

    return std::move(rel_map);
}

static auto ToOuterAll(ColumnNullableLookup& lookup) -> void {
    for (auto& nullable: lookup | std::views::values) {
        nullable.from_join = true;
    }
}

auto JoinTypeVisitor::VisitOperatorJoin(duckdb::LogicalJoin& op, ConditionRels&& rels) -> void {
    if (op.join_type == duckdb::JoinType::INNER) {
        this->VisitOperatorJoinInternal(op, duckdb::JoinType::INNER, duckdb::JoinType::INNER, std::move(rels));
    }
    else if (op.join_type == duckdb::JoinType::LEFT) {
        this->VisitOperatorJoinInternal(op, duckdb::JoinType::INNER, duckdb::JoinType::OUTER, std::move(rels));
    }
    else if (op.join_type == duckdb::JoinType::RIGHT) {
        this->VisitOperatorJoinInternal(op, duckdb::JoinType::OUTER, duckdb::JoinType::INNER, std::move(rels));
    }
    else if (op.join_type == duckdb::JoinType::OUTER) {
        this->VisitOperatorJoinInternal(op, duckdb::JoinType::OUTER, duckdb::JoinType::OUTER, {});
        ToOuterAll(this->join_type_lookup);
    }
    else if (op.join_type == duckdb::JoinType::SINGLE) {
        // scalar subquery
        this->VisitOperatorJoinInternal(op, duckdb::JoinType::INNER, duckdb::JoinType::OUTER, std::move(rels));
    }
}

static auto VisitOperatorWindow(duckdb::LogicalWindow& op, ColumnNullableLookup& parent_join_types, SampleNullableCache& sample_cache, duckdb::Connection& conn, ZmqChannel& channel) -> ColumnNullableLookup {
    ColumnNullableLookup internal_join_types{};
    JoinTypeVisitor visitor(internal_join_types, parent_join_types, sample_cache, conn, channel);

    for (auto& child: op.children) {
        visitor.VisitOperator(*child);
    }

    for (duckdb::idx_t c = 0; auto& expr: op.expressions) {
        ColumnNullableLookup::Column binding(op.window_index, c++);
        internal_join_types[binding] = ColumnExpressionVisitor::Resolve(expr, binding, internal_join_types, sample_cache);
    }

    return std::move(internal_join_types);
}

static auto VisitOperatorUnnest(duckdb::LogicalUnnest& op, ColumnNullableLookup& parent_join_types, SampleNullableCache& sample_cache, duckdb::Connection& conn, ZmqChannel& channel) -> ColumnNullableLookup {
    ColumnNullableLookup internal_join_types{};
    JoinTypeVisitor visitor(internal_join_types, parent_join_types, sample_cache, conn, channel);

    for (auto& child: op.children) {
        visitor.VisitOperator(*child);
    }

    for (duckdb::idx_t c = 0; auto& expr: op.expressions) {
        ColumnNullableLookup::Column binding{.table_index = op.unnest_index, .column_index = c++};
        internal_join_types[binding] = ColumnExpressionVisitor::Resolve(expr, binding, internal_join_types, sample_cache);
    }

    return std::move(internal_join_types);
}

static auto VisitOperatorProjection(duckdb::LogicalProjection& op, ColumnNullableLookup& parent_join_types, SampleNullableCache& sample_cache, duckdb::Connection& conn, ZmqChannel& channel) -> ColumnNullableLookup {
    ColumnNullableLookup internal_join_types{};
    JoinTypeVisitor visitor(internal_join_types, parent_join_types, sample_cache, conn, channel);

    for (auto& child: op.children) {
        visitor.VisitOperator(*child);
    }

    ColumnNullableLookup results;

    for (duckdb::idx_t c = 0; auto& expr: op.expressions) {
        ColumnNullableLookup::Column binding{.table_index = op.table_index, .column_index = c++ };
        results[binding] = ColumnExpressionVisitor::Resolve(expr, binding, internal_join_types, sample_cache);
    }

    return std::move(results);
}

static auto resolveSelectListNullabilityInternal(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ColumnNullableLookup& internal_join_types, SampleNullableCache& sample_cache, duckdb::Connection& conn, ZmqChannel& channel) -> ColumnNullabilityResult;
static auto RebindTableIndex(duckdb::idx_t table_index, ColumnNullableLookup&& internal_join_type) -> ColumnNullableLookup;

static auto VisitOperatorRecursiveCte(duckdb::LogicalRecursiveCTE& op, ColumnNullableLookup& parent_join_types, SampleNullableCache& sample_cache, duckdb::Connection& conn, ZmqChannel& channel) -> ColumnNullableLookup {
    visit_rec_cte_top: {
        auto [cte_lookup, _] = resolveSelectListNullabilityInternal(op.children[0], parent_join_types, sample_cache, conn, channel);

        // rebind to CTE table_index
        cte_lookup = RebindTableIndex(op.table_index, std::move(cte_lookup));
        parent_join_types.insert(cte_lookup.begin(), cte_lookup.end());
    }
    visit_rec_rest: {
        auto [cte_lookup, _] = resolveSelectListNullabilityInternal(op.children[1], parent_join_types, sample_cache, conn, channel);
        // rebind to CTE table_index 
        // need to pass to parent plan beacause this is recursive CTE
        cte_lookup = RebindTableIndex(op.table_index, std::move(cte_lookup));

        return std::move(cte_lookup);
    }
}

static auto VisitOperatorDMLStatementInternal(duckdb::LogicalOperator& op, duckdb::idx_t table_index, ZmqChannel& channel) -> ColumnNullableLookup {
    if (op.children.size() > 0) {
        return VisitOperatorDMLStatementInternal(*op.children.front(), table_index, channel);
    }

    ColumnNullableLookup internal_join_types{};

    if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
        auto& op_get = op.Cast<duckdb::LogicalGet>();
        auto constraints = getColumnRefNullabilities(op_get, channel);

        for (duckdb::idx_t c = 0; c < op_get.names.size(); ++c) {
            ColumnNullableLookup::Column binding{.table_index = table_index, .column_index = c };
            internal_join_types[binding] = {
                .from_field = (!constraints[c]),
                .from_join = false,
            };
        }
    }

    return std::move(internal_join_types);
}

static auto VisitOperatorDeleteStatement(duckdb::LogicalDelete& op, ZmqChannel& channel) -> ColumnNullableLookup {
    return VisitOperatorDMLStatementInternal(op, op.table_index, channel);
}

static auto VisitOperatorUpdateStatement(duckdb::LogicalUpdate& op, ZmqChannel& channel) -> ColumnNullableLookup {
    return VisitOperatorDMLStatementInternal(op, op.table_index, channel);
}

static auto VisitOperatorInsertStatement(duckdb::LogicalInsert& op, ZmqChannel& channel) -> ColumnNullableLookup {
    auto constraints = getColumnRefNullabilitiesInternal(op.table, channel);

    ColumnNullableLookup internal_join_types{};

    for (duckdb::idx_t c = 0; c < op.table.GetColumns().LogicalColumnCount(); ++c) {
        ColumnNullableLookup::Column binding{.table_index = op.table_index, .column_index = c };
        internal_join_types[binding] = {
            .from_field = (!constraints[c]),
            .from_join = false,
        };
    }

    return std::move(internal_join_types);
}

auto JoinTypeVisitor::VisitOperator(duckdb::LogicalOperator &op) -> void {
    switch (op.type) {
    case duckdb::LogicalOperatorType::LOGICAL_PROJECTION:
        {
            auto lookup = VisitOperatorProjection(op.Cast<duckdb::LogicalProjection>(), this->parent_lookup, this->sample_cache, this->conn, this->channel);
            this->join_type_lookup.insert(lookup.begin(), lookup.end());
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_WINDOW:
        {
            auto lookup = VisitOperatorWindow(op.Cast<duckdb::LogicalWindow>(), this->parent_lookup, this->sample_cache, this->conn, this->channel);
            this->join_type_lookup.insert(lookup.begin(), lookup.end());
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_UNNEST:
        {
            auto lookup = VisitOperatorUnnest(op.Cast<duckdb::LogicalUnnest>(), this->parent_lookup, this->sample_cache, this->conn, this->channel);
            this->join_type_lookup.insert(lookup.begin(), lookup.end());
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_GET:
        this->VisitOperatorGet(op.Cast<duckdb::LogicalGet>());
        break;
    case duckdb::LogicalOperatorType::LOGICAL_EXPRESSION_GET:
        this->VisitOperatorValuesGet(op.Cast<duckdb::LogicalExpressionGet>());
        break;
    case duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
        this->VisitOperatorGroupBy(op.Cast<duckdb::LogicalAggregate>());
        break;
    case duckdb::LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
        {
            auto cte_lookup = VisitOperatorRecursiveCte(op.Cast<duckdb::LogicalRecursiveCTE>(), this->parent_lookup, this->sample_cache, this->conn, this->channel);
            this->join_type_lookup.insert(cte_lookup.begin(), cte_lookup.end());
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_CTE_REF:
        this->VisitOperatorCteRef(op.Cast<duckdb::LogicalCTERef>());
        break;
    case duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
    case duckdb::LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        {
            auto& join_op = op.Cast<duckdb::LogicalComparisonJoin>();
            this->VisitOperatorJoin(join_op, VisitJoinCondition(join_op.conditions));
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_DELIM_JOIN:
        {
            auto& join_op = op.Cast<duckdb::LogicalComparisonJoin>();

            auto view = join_op.duplicate_eliminated_columns
                | std::views::filter([](auto& expr) { return expr->type == duckdb::ExpressionType::BOUND_COLUMN_REF; })
                | std::views::transform([](duckdb::unique_ptr<duckdb::Expression>& expr) { 
                    auto& c = expr->Cast<duckdb::BoundColumnRefExpression>();
                    return ColumnNullableLookup::Column{ .table_index = c.binding.table_index, .column_index = c.binding.column_index };
                })
            ;

            this->VisitOperatorJoin(join_op, VisitDelimJoinCondition(join_op, std::vector(view.begin(), view.end())));
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_ANY_JOIN:
        {
            auto& join_op = op.Cast<duckdb::LogicalAnyJoin>();

            ConditionRels rel_map{};
            binding::ConditionBindingVisitor visitor(rel_map);
            visitor.VisitExpression(&join_op.condition);

            this->VisitOperatorJoin(join_op, std::move(rel_map));
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
        this->VisitOperatorJoinInternal(op, duckdb::JoinType::OUTER, duckdb::JoinType::OUTER, {});
        break;
    case duckdb::LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
        this->VisitOperatorJoinInternal(op, duckdb::JoinType::INNER, duckdb::JoinType::INNER, {});
        break;
    case duckdb::LogicalOperatorType::LOGICAL_DELETE:
        {
            auto lookup = VisitOperatorDeleteStatement(op.Cast<duckdb::LogicalDelete>(), this->channel);
            this->join_type_lookup.insert(lookup.begin(), lookup.end());
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_UPDATE:
        {
            auto lookup = VisitOperatorUpdateStatement(op.Cast<duckdb::LogicalUpdate>(), this->channel);
            this->join_type_lookup.insert(lookup.begin(), lookup.end());
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_INSERT:
        {
            auto lookup = VisitOperatorInsertStatement(op.Cast<duckdb::LogicalInsert>(), this->channel);
            this->join_type_lookup.insert(lookup.begin(), lookup.end());
        }
        break;
    default:
        duckdb::LogicalOperatorVisitor::VisitOperatorChildren(op);
        break;
    }
}

static auto RebindTableIndex(duckdb::idx_t table_index, ColumnNullableLookup&& internal_join_type) -> ColumnNullableLookup {
    ColumnNullableLookup lookup{};

    for (auto& [internal_binding, nullable]: internal_join_type) {
        ColumnNullableLookup::Column binding = {.table_index = table_index, .column_index = internal_binding.column_index};
        lookup[binding] = nullable;
    }

    return std::move(lookup);
}

static auto resolveSetOperation(duckdb::LogicalSetOperation& op, ColumnNullableLookup& internal_join_type, SampleNullableCache& sample_cache, duckdb::Connection& conn, ZmqChannel& channel) -> ColumnNullableLookup {
    auto [left_lookup, _] = resolveSelectListNullabilityInternal(op.children[0], internal_join_type, sample_cache, conn, channel);

    if (op.type != duckdb::LogicalOperatorType::LOGICAL_UNION) {
        return std::move(left_lookup);
    }
    else {
        ColumnNullableLookup result{};

        auto [right_lookup, _] = resolveSelectListNullabilityInternal(op.children[1], internal_join_type, sample_cache, conn, channel);
        auto right_table_index = right_lookup.begin()->first.table_index;

        for (duckdb::idx_t c = 0; auto& [left_binding, left_nullable]: left_lookup) {
            ColumnNullableLookup::Column right_binding{
                .table_index = right_table_index, 
                .column_index = left_binding.column_index,
            };
            auto& right_nullable = right_lookup[right_binding];

            result[left_binding] = {
                .from_field = left_nullable.from_field || right_nullable.from_field,
                .from_join = left_nullable.from_join || right_nullable.from_join,
            };
        }

        return std::move(result);
    }
}

static auto resolveSelectListNullabilityInternal(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ColumnNullableLookup& parent_join_types, SampleNullableCache& sample_cache, duckdb::Connection& conn, ZmqChannel& channel) -> ColumnNullabilityResult {
    ColumnNullableLookup lookup;

    switch (op->type) {
    case duckdb::LogicalOperatorType::LOGICAL_ORDER_BY:
        {
            if (op->children.size() > 0) {
                return resolveSelectListNullabilityInternal(op->children[0], parent_join_types, sample_cache, conn, channel);
            }
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_UNION:
    case duckdb::LogicalOperatorType::LOGICAL_INTERSECT:
    case duckdb::LogicalOperatorType::LOGICAL_EXCEPT:
        {
            lookup = resolveSetOperation(op->Cast<duckdb::LogicalSetOperation>(), parent_join_types, sample_cache, conn, channel);
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_PROJECTION:
        {
            lookup = VisitOperatorProjection(op->Cast<duckdb::LogicalProjection>(), parent_join_types, sample_cache, conn, channel);
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_MATERIALIZED_CTE: 
        {
            auto& op_cte = op->Cast<duckdb::LogicalMaterializedCTE>();
            visit_cte: {
                auto [cte_lookup, status] = resolveSelectListNullabilityInternal(op_cte.children[0], parent_join_types, sample_cache, conn, channel);
                if (status == ResolverStatus::Unhandled) return {.nullabilities{}, .status = status};

                cte_lookup = RebindTableIndex(op_cte.table_index, std::move(cte_lookup));
                parent_join_types.insert(cte_lookup.begin(), cte_lookup.end());
            }
            visit_rest: {
                auto [rest_lookup, status] = resolveSelectListNullabilityInternal(op_cte.children[1], parent_join_types, sample_cache, conn, channel);
                if (status == ResolverStatus::Unhandled) return {.nullabilities{}, .status = status};

                lookup = std::move(rest_lookup);
            }
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
        {
            lookup = VisitOperatorRecursiveCte(op->Cast<duckdb::LogicalRecursiveCTE>(), parent_join_types, sample_cache, conn, channel);
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_DELETE:
        {
            auto delete_lookup = VisitOperatorDeleteStatement(op->Cast<duckdb::LogicalDelete>(), channel);
            parent_join_types.insert(delete_lookup.begin(), delete_lookup.end());

            // Top-level delete does not have returning field(s)
            lookup = ColumnNullableLookup{};
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_UPDATE:
        {
            auto update_lookup = VisitOperatorUpdateStatement(op->Cast<duckdb::LogicalUpdate>(), channel);
            parent_join_types.insert(update_lookup.begin(), update_lookup.end());

            // Top-level update does not have returning field(s)
            lookup = ColumnNullableLookup{};
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_INSERT:
        {
            auto update_lookup = VisitOperatorInsertStatement(op->Cast<duckdb::LogicalInsert>(), channel);
            parent_join_types.insert(update_lookup.begin(), update_lookup.end());

            // Top-level update does not have returning field(s)
            lookup = ColumnNullableLookup{};
        }
        break;
    default: 
        channel.warn(std::format("[TODO] Not implemented plan root: {}", magic_enum::enum_name(op->type)));

        return {
            .nullabilities{},
            .status = ResolverStatus::Unhandled,
        };
    }

    return {
        .nullabilities = std::move(lookup),
        .status = ResolverStatus::Handled,
    };
}

auto resolveSelectListNullability(duckdb::unique_ptr<duckdb::LogicalOperator>& op, duckdb::Connection& conn, ZmqChannel& channel) -> ColumnNullabilityResult {
    ColumnNullableLookup internal_join_types{};
    SampleNullableCache sample_cach{};
    
    return resolveSelectListNullabilityInternal(op, internal_join_types, sample_cach, conn, channel);
}

}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include "duckdb_catch2_fmt.hpp"
#include "../resolver.select_statement_nullable/run.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/matchers/catch_matchers_vector.hpp>

using namespace worker;
using namespace Catch::Matchers;

auto runResolveSelectListNullability(const std::string& sql, std::vector<std::string>&& schemas, const std::vector<ColumnBindingPair>& expects) -> void {
        duckdb::DuckDB db(nullptr);
        duckdb::Connection conn(db);

        for (auto& schema: schemas) {
            conn.Query(schema);
        }

        auto stmts = conn.ExtractStatements(sql);

        ColumnNullabilityResult join_type_result;
        try {
            conn.BeginTransaction();

            auto walk_result = walkSQLStatement(stmts[0], ZmqChannel::unitTestChannel());
            auto bound_result = bindTypeToStatement(*conn.context, std::move(stmts[0]->Copy()), walk_result.names, walk_result.examples);
            auto channel = ZmqChannel::unitTestChannel();
            join_type_result = resolveSelectListNullability(bound_result.stmt.plan, conn, channel);

            conn.Commit();
        }
        catch (...) {
            conn.Rollback();
            throw;
        }

    handled: {
        INFO("Result status");
        REQUIRE(magic_enum::enum_name(join_type_result.status) != magic_enum::enum_name(ResolverStatus::Unhandled));
    }
    Result_size: {
        INFO("Result size");
        REQUIRE(join_type_result.nullabilities.size() == expects.size());
    }
    Result_items: {    
        auto view = join_type_result.nullabilities | std::views::keys;
        std::vector<ColumnNullableLookup::Column> bindings_result(view.begin(), view.end());

        for (int i = 0; i < expects.size(); ++i) {
            auto expect = expects[i];

            INFO(std::format("has column binding#{}", i+1));
            CHECK_THAT(bindings_result, VectorContains(ColumnNullableLookup::Column::from(expect.binding)));
            
            INFO(std::format("column nullability (field)#{}", i+1));
            CHECK(join_type_result.nullabilities[expect.binding].from_field == expect.nullable.from_field);
            
            INFO(std::format("column nullability (join)", i+1));
            CHECK(join_type_result.nullabilities[expect.binding].from_join == expect.nullable.from_join);
        }
    }
}

#endif