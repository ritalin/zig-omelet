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

static auto getColumnRefNullabilities(const duckdb::LogicalGet& op, ZmqChannel& channel) -> ColumnRefNullabilityMap {
    auto bind_info = op.function.get_bind_info(op.bind_data);
    
    if (! bind_info.table) {
        channel.warn(std::format("[TODO] cannot find table catalog: (index: {})", op.table_index));
        return {};
    }

    auto constraints = 
        bind_info.table->GetConstraints()
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

static auto resolveSelectListNullabilityInternal(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ColumnNullableLookup& internal_join_type, SampleNullableCache& sample_cache, duckdb::Connection& conn, ZmqChannel& channel) -> ColumnNullableLookup;
static auto RebindTableIndex(duckdb::idx_t table_index, ColumnNullableLookup&& internal_join_type) -> ColumnNullableLookup;

static auto VisitOperatorRecursiveCte(duckdb::LogicalRecursiveCTE& op, ColumnNullableLookup& parent_join_types, SampleNullableCache& sample_cache, duckdb::Connection& conn, ZmqChannel& channel) -> ColumnNullableLookup {
    visit_rec_cte_top: {
        auto cte_lookup = resolveSelectListNullabilityInternal(op.children[0], parent_join_types, sample_cache, conn, channel);
        // rebind to CTE table_index
        cte_lookup = RebindTableIndex(op.table_index, std::move(cte_lookup));
        parent_join_types.insert(cte_lookup.begin(), cte_lookup.end());
    }
    visit_rec_rest: {
        auto cte_lookup = resolveSelectListNullabilityInternal(op.children[1], parent_join_types, sample_cache, conn, channel);
        // rebind to CTE table_index 
        // need to pass to parent plan beacause this is recursive CTE
        cte_lookup = RebindTableIndex(op.table_index, std::move(cte_lookup));

        return std::move(cte_lookup);
    }
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
    auto left_lookup = resolveSelectListNullabilityInternal(op.children[0], internal_join_type, sample_cache, conn, channel);

    if (op.type != duckdb::LogicalOperatorType::LOGICAL_UNION) {
        return std::move(left_lookup);
    }
    else {
        ColumnNullableLookup result{};

        auto right_lookup = resolveSelectListNullabilityInternal(op.children[1], internal_join_type, sample_cache, conn, channel);
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

static auto resolveSelectListNullabilityInternal(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ColumnNullableLookup& parent_join_type, SampleNullableCache& sample_cache, duckdb::Connection& conn, ZmqChannel& channel) -> ColumnNullableLookup {
    ColumnNullableLookup lookup;

    switch (op->type) {
    case duckdb::LogicalOperatorType::LOGICAL_ORDER_BY:
        {
            if (op->children.size() > 0) {
                return resolveSelectListNullabilityInternal(op->children[0], parent_join_type, sample_cache, conn, channel);
            }
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_UNION:
    case duckdb::LogicalOperatorType::LOGICAL_INTERSECT:
    case duckdb::LogicalOperatorType::LOGICAL_EXCEPT:
        {
            lookup = resolveSetOperation(op->Cast<duckdb::LogicalSetOperation>(), parent_join_type, sample_cache, conn, channel);
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_PROJECTION:
        {
            lookup = VisitOperatorProjection(op->Cast<duckdb::LogicalProjection>(), parent_join_type, sample_cache, conn, channel);
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_MATERIALIZED_CTE: 
        {
            auto& op_cte = op->Cast<duckdb::LogicalMaterializedCTE>();
            visit_cte: {
                auto cte_lookup = resolveSelectListNullabilityInternal(op_cte.children[0], parent_join_type, sample_cache, conn, channel);
                cte_lookup = RebindTableIndex(op_cte.table_index, std::move(cte_lookup));
                parent_join_type.insert(cte_lookup.begin(), cte_lookup.end());
            }
            visit_rest: {
                lookup = resolveSelectListNullabilityInternal(op_cte.children[1], parent_join_type, sample_cache, conn, channel);
            }
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
        {
            lookup = VisitOperatorRecursiveCte(op->Cast<duckdb::LogicalRecursiveCTE>(), parent_join_type, sample_cache, conn, channel);
        }
        break;
    default: 
        channel.warn(std::format("[TODO] Not implemented plan root: {}", magic_enum::enum_name(op->type)));
    }

    return std::move(lookup);
}

auto resolveSelectListNullability(duckdb::unique_ptr<duckdb::LogicalOperator>& op, duckdb::Connection& conn, ZmqChannel& channel) -> ColumnNullableLookup {
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

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/matchers/catch_matchers_vector.hpp>

using namespace worker;
using namespace Catch::Matchers;

struct ColumnBindingPair {
    duckdb::ColumnBinding binding;
    ColumnNullableLookup::Item nullable;
};

static auto runResolveSelectListNullability(const std::string& sql, std::vector<std::string>&& schemas, const std::vector<ColumnBindingPair>& expects) -> void {
        duckdb::DuckDB db(nullptr);
        duckdb::Connection conn(db);

        for (auto& schema: schemas) {
            conn.Query(schema);
        }

        auto stmts = conn.ExtractStatements(sql);

        ColumnNullableLookup join_type_result;
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

    Result_size: {
        INFO("Result size");
        REQUIRE(join_type_result.size() == expects.size());
    }
    Result_items: {    
        auto view = join_type_result | std::views::keys;
        std::vector<ColumnNullableLookup::Column> bindings_result(view.begin(), view.end());

        for (int i = 0; i < expects.size(); ++i) {
            auto expect = expects[i];

            INFO(std::format("has column binding#{}", i+1));
            CHECK_THAT(bindings_result, VectorContains(ColumnNullableLookup::Column::from(expect.binding)));
            
            INFO(std::format("column nullability (field)#{}", i+1));
            CHECK(join_type_result[expect.binding].from_field == expect.nullable.from_field);
            
            INFO(std::format("column nullability (join)", i+1));
            CHECK(join_type_result[expect.binding].from_join == expect.nullable.from_join);
        }
    }
}

TEST_CASE("ResolveNullable::fromless") {
    SECTION("basic") {
        std::string sql(R"#(
            select 123, 'abc'
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {}, expects);
    }
}

TEST_CASE("ResolveNullable::joinless") {
    SECTION("basic") {
        std::string schema("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Bar
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema}, expects);
    }
    SECTION("unordered select list") {
        std::string schema("CREATE TABLE Bar (id int primary key, value VARCHAR not null, remarks VARCHAR)");
        std::string sql(R"#(
            select value, remarks, id from Bar
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(1, 2), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema}, expects);
    }
    SECTION("with unary op of nallble") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select -xys from Foo");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = true, .from_join = false}},
        };

        runResolveSelectListNullability(sql, {schema}, expects);
    }
    SECTION("aggregate with filter") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select sum($val::int) filter (fmod(id, $div::int) > $rem::int) as a from Foo");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = true, .from_join = false}},
        };

        runResolveSelectListNullability(sql, {schema}, expects);
    }
}

TEST_CASE("ResolveNullable::Inner join") {
    SECTION("basic") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            join Bar on Foo.id = Bar.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 4), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 5), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("nullable key") {
        std::string schema_1("CREATE TABLE Foo (id int, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            join Bar on Foo.id = Bar.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 5), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("join twice") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            join Bar b1 on Foo.id = b1.id
            join Bar b2 on Foo.id = b2.id    
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 4), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 5), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 6), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 7), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
}

TEST_CASE("ResolveNullable::outer join") {
    SECTION("left outer") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            left outer join Bar on Foo.id = Bar.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 5), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Left outer join twice") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            left outer join Bar b1 on Foo.id = b1.id
            left outer join Bar b2 on Foo.id = b2.id    
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 5), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 6), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 7), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Right outer") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            right outer join Bar on Foo.id = Bar.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 1), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 2), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 3), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 4), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 5), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
}

TEST_CASE("ResolveNullable::Inner + outer") {
    SECTION("Inner -> outer") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            join Bar b1 on Foo.id = b1.id
            left outer join Bar b2 on Foo.id = b2.id    
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 4), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 5), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 6), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 7), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Outer -> inner#1") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            left outer join Bar b1 on Foo.id = b1.id
            join Bar b2 on Foo.id = b2.id    
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 5), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 6), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 7), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Outer -> inner#2") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            left outer join Bar b1 on Foo.id = b1.id
            join Bar b2 on b1.id = b2.id    
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 5), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 6), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 7), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
}

TEST_CASE("ResolveNullable::scalar subquery") {
    SECTION("single left outer join") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select 
                Foo.id,
                (
                    select Bar.value from Bar
                    where bar.id = Foo.id
                ) as v
            from Foo 
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
}

TEST_CASE("ResolveNullable::Cross join") {
    SECTION("basic") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            cross join Bar
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 4), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 5), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Cross -> outer") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            cross join Bar b1
            left outer join Bar b2 on Foo.id = b2.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 4), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 5), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 6), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 7), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
}

TEST_CASE("ResolveNullable::Full outer join") {
    SECTION("basic") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            full outer join Bar on Foo.id = Bar.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 1), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 2), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 3), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 5), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Inner -> full outer#1") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            join Bar b1 on Foo.id = b1.id
            full outer join Bar b2 on Foo.id = b2.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 2), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 3), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 5), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 6), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 7), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Inner -> full outer#2") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            join Bar b1 on Foo.id = b1.id
            full outer join Bar b2 on b1.id = b2.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 2), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 3), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 5), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 6), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 7), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Full outer -> inner#1") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            full outer join Bar b2 on Foo.id = b2.id
            join Bar b1 on Foo.id = b1.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 2), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 3), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 5), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 6), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 7), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Full outer -> inner#2") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            full outer join Bar b2 on Foo.id = b2.id
            join Bar b1 on b2.id = b1.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 2), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 3), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 5), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 6), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 7), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
}

TEST_CASE("ResolveNullable::Positional join") {
    SECTION("basic") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            positional join Bar
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 1), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 2), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 3), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 5), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
}

TEST_CASE("ResolveNullable::lateral join") {
    SECTION("Inner join lateral#1") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select *
            from Foo 
            join lateral (
            select * from Bar
            where Bar.value = Foo.id
            ) on true
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(8, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 4), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 5), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Inner join lateral#2 (unordered select list)") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select v.k1, v.value, Foo.*, v.k2
            from Foo 
            join lateral (
            select value, id as k1, id as k2 from Bar
            where Bar.value = Foo.id
            ) v on true
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(8, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 2), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 3), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 4), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 5), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 6), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Inner join lateral#3 (nullable key)") {
        std::string schema_1("CREATE TABLE Foo (id int, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select *
            from Foo 
            join lateral (
            select * from Bar
            where Bar.value = Foo.id
            ) on true
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(8, 0), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(8, 5), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Outer join lateral") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select *
            from Foo 
            left outer join lateral (
            select * from Bar
            where Bar.value = Foo.id
            ) on true
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(8, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(8, 5), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
}

TEST_CASE("join + subquery") {
    SECTION("Inner join with subquery#1") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select *
            from Foo 
            join (
            select * from Bar
            ) b1 on b1.value = Foo.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(8, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 4), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 5), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Inner join with subquery#2") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string schema_3("CREATE TABLE Baz (id int primary key, order_date DATE not null)");
        std::string sql(R"#(
            select *
            from Foo 
            join (
            select * from Bar
            join Baz on Bar.id = Baz.id
            ) b1 on b1.value = Foo.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(9, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(9, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(9, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(9, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(9, 4), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(9, 5), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(9, 6), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(9, 7), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2, schema_3}, expects);
    }
}

TEST_CASE("ResolveNullable::exists clause") {
    SECTION("mark join") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string schema_3("CREATE TABLE Baz (id int primary key, order_date DATE not null)");
        std::string sql(R"#(
            with ph as (select $k::int as k)
            select Bar.* from Bar
            join lateral (
                select * from Baz
                cross join ph
                where 
                    Baz.id = Bar.id
                    and exists (
                        from Foo 
                        where 
                            Foo.id = Baz.id
                            and kind = ph.k
                    )
            ) v on true
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(22, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(22, 1), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2, schema_3}, expects);
    }
}

TEST_CASE("ResolveNullable::With order by query") {
    SECTION("basic") {
        std::string schema_1("CREATE TABLE Foo (id int, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            join Bar on Foo.id = Bar.id
            order by Foo.id, bar.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 5), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("subquery with order by clause") {
        std::string schema_1("CREATE TABLE Foo (id int, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from (
                select * from Foo
                order by Foo.id
            ) v
            join Bar on v.id = Bar.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(8, 0), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(8, 5), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
}

TEST_CASE("ResolveNullable::With group by query") {
    SECTION("basic") {
        std::string schema_1("CREATE TABLE Foo (id int, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select Bar.id, Foo.id, count(Foo.kind) as k, count(xys) as s from Foo
            join Bar on Foo.id = Bar.id
            group by Bar.id, Foo.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 1), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 3), .nullable = {.from_field = true, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("rollup") {
        std::string schema("CREATE TABLE Point (id int, x int not null, y int, z int not null)");
        std::string sql("SELECT x, y, GROUPING(x, y), GROUPING(x), sum(z) FROM Point GROUP BY ROLLUP(x, y)");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(1, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(1, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(1, 4), .nullable = {.from_field = true, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema}, expects);
    }
}

TEST_CASE("ResolveNullable::Builtin unnest function") {
    SECTION("Expand list#1") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select 42::int as x, xys, unnest([1, 2, 3, 5, 7, 11]) from Foo");

        // Note: list initializer([]) is a alias for list_value function, and is not operator.
        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 2), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
    SECTION("Expand list#2") {
        std::string sql("select 42::int as x, unnest('[1, 2, 3, 5, 7]'::json::int[]) as x");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("Result of generate_series") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select null::int as x, xys, unnest(generate_series(1, 20, 3)) from Foo");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 2), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
}

TEST_CASE("ResolveNullable::Builtin window function") {
    SECTION("row_number") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select id, row_number() over (order by id desc) as a from Foo");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
    SECTION("ntile") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select xys, ntile($bucket) over () as a from Foo");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
    SECTION("lag") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select id, lag(id, $offset, $value_def::int) over (partition by kind) as a from Foo");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
}

TEST_CASE("ResolveNullable::table function") {
    SECTION("read_json#1 struct") {
        std::string sql(R"#(
            select 
                unnest(h, recursive := true), 
                unnest(h), 
                t.* 
            from read_json("$dataset" := '_dataset-examples/struct_sample.json') t(h, i, j)
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 2), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 3), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 4), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 5), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 6), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 7), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("read_json#2 (derived table/unnest in outermost)") {
        std::string sql(R"#(
            select 
                unnest(v.h, recursive := true), unnest(v.h), v.* 
            from (
                select * 
                from read_json("$dataset" := '_dataset-examples/struct_sample.json') t(h, i, j)
            ) v
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(7, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 1), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 2), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 3), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 4), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 5), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 6), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 7), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("read_json#3 (derived table/unnest anything)") {
        std::string sql(R"#(
            select 
                unnest(data_1, recursive := true), unnest(data_2)
            from (
                select unnest(j)
                from read_json("$dataset" := '_dataset-examples/struct_sample_2.json') t(j)
            )
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(7, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 1), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 2), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 3), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 4), .nullable = {.from_field = true, .from_join = false}},
        };

        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("read_json#3 (derived table/unnest descendant directly)") {
        std::string sql(R"#(
            select 
                unnest(j.data_1.v)
            from read_json("$dataset" := '_dataset-examples/struct_sample_2.json') t(j)
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("read_json#5 list") {
        std::string sql(R"#(
            select 
                k, v1, unnest(v1), v2, v3, unnest(v3)
            from read_json("$dataset" := '_dataset-examples/list_sample.json') t(k, v1, v2, v3)
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 2), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 3), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 4), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 5), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("read_json#6 from CTE#1") {
        std::string sql(R"#(
            with source as (
                select *
                from read_json("$dataset" := '_dataset-examples/struct_sample_2.json') t(j)
            )
            select unnest(s.j.data_1, recursive := true), unnest(s.j.data_2)
            from source s
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(7, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 1), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 2), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 3), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 4), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("read_json#6 from CTE#2") {
        std::string sql(R"#(
            with source as (
                select unnest(j)
                from read_json("$dataset" := '_dataset-examples/struct_sample_2.json') t(j)
            )
            select unnest(s.data_1, recursive := true), unnest(s.data_2)
            from source s
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(7, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 1), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 2), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 3), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 4), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("range#1") {
        std::string sql(R"#(
            select u.*
            from range("$start" := 1::bigint, "$stop" := 50, 3) u(id)
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("range#2 (correlated column)") {
        std::string sql(R"#(
            select t.*, u.id
            from (values (1, 2), (2, 3)) t(x, y)
            cross join lateral range(x, "$stop" := 50, y) u(id)
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(15, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(15, 1), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(15, 2), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
}

TEST_CASE("ResolveNullable::CTE") {
    SECTION("default") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            with v as (
                select id, xys, kind from Foo
            )
            select xys, id from v
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(7, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 1), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
    SECTION("With non materialized CTE") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            with v as not materialized (
                select id, xys, kind from Foo
            )
            select xys, id from v
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(7, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 1), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
}

TEST_CASE("ResolveNullable::Materialized CTE") {
    SECTION("basic") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            with v as materialized (
                select id, xys, kind from Foo
            )
            select xys, id from v
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(9, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(9, 1), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
    SECTION("CTEx2") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int, value VARCHAR not null)");
        std::string schema_3("CREATE TABLE Point (id int, x int not null, y int, z int not null)");
        std::string sql(R"#(
            with
                v as materialized (
                    select Foo.id, Bar.id, xys, kind, a from Foo
                    join Bar on Foo.id = Bar.id
                    cross join (
                        select $a::int as a
                    )
                ),
                v2 as materialized (
                    select $b::text as b, x from Point
                )
            select xys, id, b, x, a from v
            cross join v2
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(26, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(26, 1), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(26, 2), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(26, 3), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(26, 4), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema_1, schema_2, schema_3}, expects);
    }
    SECTION("nested CTE") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int, value VARCHAR not null)");
        std::string sql(R"#(
            with
                v as materialized (
                    select Foo.id, Bar.id, xys, kind, a from Foo
                    join Bar on Foo.id = Bar.id
                    cross join (
                        select $a::int as a
                    )
                ),
                v2 as materialized (
                    select id, id_1 from v
                )
            select id_1, id from v2
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(25, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(25, 1), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("reference from subquery") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            with v as materialized (
                select id, xys, kind from Foo
            )
            select xys, id from (
                select * from v
            )
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(15, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(15, 1), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
}

TEST_CASE("ResolveNullable::Recursive CTE") {
    SECTION("default CTE") {
        std::string sql(R"#(
            with recursive t(n, k) AS (
                VALUES (0, $target_date::date)
                UNION ALL
                SELECT n+1, k+1 FROM t WHERE n < $max_value::int
            )
            SELECT k, n FROM t
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(15, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(15, 1), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("default CTEx2") {
        std::string sql(R"#(
            with recursive 
                t(n, k) AS (
                    VALUES (0, current_date)
                    UNION ALL
                    SELECT n+1, k+2 FROM t WHERE n < $max_value::int
                ),
                t2(m, h) AS (
                    VALUES ($min_value::int, $val::int)
                    UNION ALL
                    SELECT m*2, h+1 FROM t2 WHERE m < $max_value2::int
                )
            SELECT n, h, k, m FROM t cross join t2
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(30, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(30, 1), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(30, 2), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(30, 3), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("default CTE (nested)") {
        std::string sql(R"#(
            with recursive
                t(n, k) AS (
                    VALUES (0, 42)
                    UNION ALL
                    SELECT n+1, k*2 FROM t WHERE n < $max_value::int
                ),
                t2(m, h) as (
                    select n + $delta::int as m, k from t
                    union all
                    select m*2, h-1 from t2 where m < $max_value2::int
                )
            SELECT h, m FROM t2 
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(29, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(29, 1), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
}

TEST_CASE("ResolveNullable::Recursive materialized CTE") {
    SECTION("Matirialized CTE") {
        std::string sql(R"#(
            with recursive t(n, k) AS materialized (
                VALUES (1, $min_value::int)
                UNION ALL
                SELECT n+1, k-1 FROM t WHERE n < $max_value::int
            )
            SELECT k, n FROM t
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(17, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(17, 1), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("Matirialized CTEx2") {
        std::string sql(R"#(
            with recursive 
                t(n, k) AS materialized (
                    VALUES (0, current_date)
                    UNION ALL
                    SELECT n+1, k+2 FROM t WHERE n < $max_value::int
                ),
                t2(m, h) AS materialized (
                    VALUES ($min_value::int, $val::int)
                    UNION ALL
                    SELECT m*2, h+1 FROM t2 WHERE m < $max_value2::int
                )
            SELECT n, h, k, m FROM t cross join t2
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(34, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(34, 1), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(34, 2), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(34, 3), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("materialized CTE (nested)") {
        std::string sql(R"#(
            with recursive
                t(n, k) AS materialized (
                    VALUES (0, 42)
                    UNION ALL
                    SELECT n+1, k*2 FROM t WHERE n < $max_value::int
                ),
                t2(m, h) as materialized (
                    select n + $delta::int as m, k from t
                    union all
                    select m*2, h-1 from t2 where m < $max_value2::int
                )
            SELECT h, m FROM t2 
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(33, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(33, 1), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
}

TEST_CASE("ResolveNullable::With combining operation") {
    SECTION("union#1 (bottom nullable)") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int, value VARCHAR not null)");
        std::string sql(R"#(
            select id from Foo where id > $n1
            union all
            select id from Bar where id <= $n2
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("union#2") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            select id from Foo where id > $n1
            union all
            select id from Foo where id <= $n2
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema_1}, expects);
    }
    SECTION("intersect#1 (bottom nullable)") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int, value VARCHAR not null)");
        std::string sql(R"#(
            select id from Foo where id > $n1
            intersect all
            select id from Bar where id <= $n2
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("intersect#2 (top nullable)") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int, value VARCHAR not null)");
        std::string sql(R"#(
            select id from Bar where id > $n1
            intersect all
            select id from Foo where id <= $n2
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("except#1 (top nullable)") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int, value VARCHAR not null)");
        std::string sql(R"#(
            select id from Foo where id > $n1
            except all
            select id from Bar where id <= $n2
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("except#2 (bottom nullable)") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int, value VARCHAR not null)");
        std::string sql(R"#(
            select id from Bar where id > $n1
            except all
            select id from Foo where id <= $n2
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
}

#endif