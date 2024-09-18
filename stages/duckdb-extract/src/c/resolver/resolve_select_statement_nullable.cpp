#include <ranges>
#include <iostream>

#include <duckdb.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_aggregate.hpp>
#include <duckdb/planner/operator/logical_cteref.hpp>
#include <duckdb/planner/operator/logical_delim_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_comparison_join.hpp>
#include <duckdb/planner/operator/logical_any_join.hpp>
#include <duckdb/planner/operator/logical_materialized_cte.hpp>
#include <duckdb/planner/operator/logical_set_operation.hpp>
#include <duckdb/planner/operator/logical_window.hpp>
#include <duckdb/planner/bound_tableref.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>

#include <magic_enum/magic_enum.hpp>

#include "duckdb_logical_visitors.hpp"

namespace worker {

using ColumnRefNullabilityMap = std::unordered_map<duckdb::idx_t, bool>;

static auto EvaluateNullability(duckdb::JoinType rel_join_type, const JoinTypeVisitor::ConditionRels& rel_map, const NullableLookup& internal_lookup, const NullableLookup& lookup) -> bool {
    if (rel_join_type == duckdb::JoinType::OUTER) return true;

    return std::ranges::any_of(rel_map | std::views::values, [&](const auto& to) { return lookup[to].shouldNulls(); });
}

static auto tryGetColumnRefNullabilities(const duckdb::LogicalGet& op, ZmqChannel& channel) -> ColumnRefNullabilityMap {
    if (!op.function.get_bind_info) {
        channel.warn(std::format("[TODO] cannot find table catalog: (index: {})", op.table_index));
        return {};
    }

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

auto JoinTypeVisitor::VisitOperatorGet(const duckdb::LogicalGet& op) -> void {
    auto constraints = tryGetColumnRefNullabilities(op, this->channel);

    auto sz = constraints.size();

    for (duckdb::idx_t i = 0; auto id: op.GetColumnIds()) {
        const NullableLookup::Column col{
            .table_index = op.table_index, 
            .column_index = i++
        };
        
        this->join_type_lookup[col] = NullableLookup::Nullability{
            .from_field = (!constraints[id]),
            .from_join = false
        };
    }
}

auto JoinTypeVisitor::VisitOperatorGroupBy(duckdb::LogicalAggregate& op) -> void {
    NullableLookup internal_join_types{};
    JoinTypeVisitor visitor(internal_join_types, this->join_type_lookup, this->cte_columns, this->channel);

    for (auto& c: op.children) {
        visitor.VisitOperator(*c);
    }

    group_index: {
        for (duckdb::idx_t c = 0; auto& expr: op.groups) {
            NullableLookup::Column to_binding{
                .table_index = op.group_index, 
                .column_index = c, 
            };

            this->join_type_lookup[to_binding] = ColumnExpressionVisitor::Resolve(expr, internal_join_types);
            ++c;
        }
    }
    aggregate_index: {
        for (duckdb::idx_t c = 0; auto& expr: op.expressions) {
            NullableLookup::Column to_binding{
                .table_index = op.aggregate_index, 
                .column_index = c, 
            };

            this->join_type_lookup[to_binding] = ColumnExpressionVisitor::Resolve(expr, internal_join_types);
            ++c;
        }
    }
    groupings_index: {
        // for (duckdb::idx_t c = 0; c < op.grouping_functions.size(); ++c) {
        for (duckdb::idx_t c = 0; auto& expr: op.grouping_functions) {
            NullableLookup::Column to_binding{
                .table_index = op.groupings_index, 
                .column_index = c++, 
            };
            this->join_type_lookup[to_binding] = {.from_field = true, .from_join = false};
        }
    }
}

auto JoinTypeVisitor::VisitOperatorCteRef(duckdb::LogicalCTERef& op) -> void {
    if (!this->cte_columns.get().contains(op.cte_index)) return;

    auto& binding_vec = this->cte_columns.get().at(op.cte_index);
    auto bindings_view = 
        binding_vec
        | std::views::transform([](const auto& x) {
            return std::pair<std::string, NullableLookup::Column>{x.name, x.binding};
        })
    ;
    auto lookup = std::unordered_map<std::string, NullableLookup::Column>(bindings_view.begin(), bindings_view.end());

    for (duckdb::idx_t c = 0; auto& col_name: op.bound_columns) {
        if (lookup.contains(col_name)) {
            auto& from_binding = lookup[col_name];
            auto nullability = this->parent_lookup[from_binding];

            NullableLookup::Column to_binding{
                .table_index = op.table_index, 
                .column_index = c, 
            };
            this->join_type_lookup[to_binding] = nullability;
        }
        ++c;
    }
}

auto JoinTypeVisitor::VisitOperatorCondition(duckdb::LogicalOperator &op, duckdb::JoinType join_type, const ConditionRels& rels) -> NullableLookup {
    NullableLookup internal_join_types{};
    JoinTypeVisitor visitor(internal_join_types, this->parent_lookup, this->cte_columns, this->channel);

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
            NullableLookup::Column col{
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
        DelimGetBindingVisitor(JoinTypeVisitor::ConditionRels& rels, std::vector<NullableLookup::Column>&& map_to)
            : condition_rels(rels), map_to(map_to)
        {
        }
    public:
        auto VisitOperator(duckdb::LogicalOperator &op) -> void {
            if (op.type == duckdb::LogicalOperatorType::LOGICAL_DELIM_GET) {
                auto& op_get = op.Cast<duckdb::LogicalDelimGet>();
                for (duckdb::idx_t i = 0; i < op_get.chunk_types.size(); ++i) {
                    NullableLookup::Column from{
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
        std::vector<NullableLookup::Column> map_to;
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

static auto VisitDelimJoinCondition(duckdb::LogicalComparisonJoin& op, std::vector<NullableLookup::Column>&& map_to) -> JoinTypeVisitor::ConditionRels {
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

static auto ToOuterAll(NullableLookup& lookup) -> void {
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

static auto VisitOperatorWindow(duckdb::LogicalWindow& op, NullableLookup& parent_join_types, const CteColumnBindingsRef& cte_columns, ZmqChannel& channel) -> NullableLookup {
    NullableLookup internal_join_types{};
    JoinTypeVisitor visitor(internal_join_types, parent_join_types, cte_columns, channel);

    for (auto& child: op.children) {
        visitor.VisitOperator(*child);
    }

    for (duckdb::idx_t i = 0; auto& expr: op.expressions) {
        duckdb::ColumnBinding binding(op.window_index, i++);
        internal_join_types[binding] = ColumnExpressionVisitor::Resolve(expr, internal_join_types);
    }

    return std::move(internal_join_types);
}

static auto VisitOperatorProjection(duckdb::LogicalProjection& op, NullableLookup& parent_join_types, const CteColumnBindingsRef& cte_columns, ZmqChannel& channel) -> NullableLookup {
    NullableLookup internal_join_types{};
    JoinTypeVisitor visitor(internal_join_types, parent_join_types, cte_columns, channel);

    for (auto& child: op.children) {
        visitor.VisitOperator(*child);
    }

    NullableLookup results;

    for (duckdb::idx_t i = 0; auto& expr: op.expressions) {
        duckdb::ColumnBinding binding(op.table_index, i++);
        results[binding] = ColumnExpressionVisitor::Resolve(expr, internal_join_types);
    }

    return std::move(results);
}

auto JoinTypeVisitor::VisitOperator(duckdb::LogicalOperator &op) -> void {
    switch (op.type) {
    case duckdb::LogicalOperatorType::LOGICAL_PROJECTION:
        {
            auto lookup = VisitOperatorProjection(op.Cast<duckdb::LogicalProjection>(), this->parent_lookup, this->cte_columns, this->channel);
            this->join_type_lookup.insert(lookup.begin(), lookup.end());
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_WINDOW:
        {
            auto lookup = VisitOperatorWindow(op.Cast<duckdb::LogicalWindow>(), this->parent_lookup, this->cte_columns, this->channel);
            this->join_type_lookup.insert(lookup.begin(), lookup.end());
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_GET:
        this->VisitOperatorGet(op.Cast<duckdb::LogicalGet>());
        break;
    case duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
        this->VisitOperatorGroupBy(op.Cast<duckdb::LogicalAggregate>());
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
                    return NullableLookup::Column{ .table_index = c.binding.table_index, .column_index = c.binding.column_index };
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

static auto RebindTableIndex(duckdb::idx_t table_index, NullableLookup&& internal_join_type) -> NullableLookup {
    NullableLookup lookup{};

    for (auto& [internal_binding, nullable]: internal_join_type) {
        NullableLookup::Column binding = {.table_index = table_index, .column_index = internal_binding.column_index};
        lookup[binding] = nullable;
    }

    return std::move(lookup);
}

static auto resolveSelectListNullabilityInternal(duckdb::unique_ptr<duckdb::LogicalOperator>& op, NullableLookup& internal_join_type, CteColumnBindings& cte_columns, ZmqChannel& channel) -> NullableLookup;

static auto resolveSetOperation(duckdb::LogicalSetOperation& op, NullableLookup& internal_join_type, CteColumnBindings& cte_columns, ZmqChannel& channel) -> NullableLookup {
    auto left_lookup = resolveSelectListNullabilityInternal(op.children[0], internal_join_type, cte_columns, channel);

    if (op.type != duckdb::LogicalOperatorType::LOGICAL_UNION) {
        return std::move(left_lookup);
    }
    else {
        NullableLookup result{};

        auto right_lookup = resolveSelectListNullabilityInternal(op.children[1], internal_join_type, cte_columns, channel);
        auto right_table_index = right_lookup.begin()->first.table_index;

        for (duckdb::idx_t c = 0; auto& [left_binding, left_nullable]: left_lookup) {
            NullableLookup::Column right_binding{
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


static auto UpdateCteColumns(const duckdb::idx_t table_index, std::vector<duckdb::unique_ptr<duckdb::Expression>>& exprs) -> std::vector<CteColumnEntry> {
    std::vector<CteColumnEntry> entries;
    entries.reserve(exprs.size());

    std::unordered_map<std::string, uint32_t> name_lookup{};

    for (duckdb::idx_t c = 0; auto& expr: exprs) {
        auto name = ColumnNameVisitor::Resolve(expr);
        auto& count = name_lookup[name];

        if (count > 0) {
            name = std::format("{}_{}", name, count);
        }
        count += 1;
        
        entries.emplace_back(CteColumnEntry{
            .name = std::move(name), 
            .binding = {.table_index = table_index, .column_index = c++}
        });
    }

    return std::move(entries);
}

static auto resolveSelectListNullabilityInternal(duckdb::unique_ptr<duckdb::LogicalOperator>& op, NullableLookup& internal_join_type, CteColumnBindings& cte_columns, ZmqChannel& channel) -> NullableLookup {
    NullableLookup lookup;

    switch (op->type) {
    case duckdb::LogicalOperatorType::LOGICAL_ORDER_BY:
        {
            if (op->children.size() > 0) {
                return resolveSelectListNullabilityInternal(op->children[0], internal_join_type, cte_columns, channel);
            }
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_UNION:
    case duckdb::LogicalOperatorType::LOGICAL_INTERSECT:
    case duckdb::LogicalOperatorType::LOGICAL_EXCEPT:
        {
            lookup = resolveSetOperation(op->Cast<duckdb::LogicalSetOperation>(), internal_join_type, cte_columns, channel);
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_PROJECTION:
        {
            lookup = VisitOperatorProjection(op->Cast<duckdb::LogicalProjection>(), internal_join_type, cte_columns, channel);
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_MATERIALIZED_CTE: 
        {
            auto& op_cte = op->Cast<duckdb::LogicalMaterializedCTE>();
            visit_cte: {
                cte_columns[op_cte.table_index] = UpdateCteColumns(op_cte.table_index, op_cte.children[0]->expressions);

                auto cte_lookup = resolveSelectListNullabilityInternal(op_cte.children[0], internal_join_type, cte_columns, channel);
                cte_lookup = RebindTableIndex(op_cte.table_index, std::move(cte_lookup));
                internal_join_type.insert(cte_lookup.begin(), cte_lookup.end());
            }
            visit_rest: {
                lookup = resolveSelectListNullabilityInternal(op_cte.children[1], internal_join_type, cte_columns, channel);
            }
        }
        break;
    default: 
        channel.warn(std::format("[TODO] Not implemented plan root: {}", magic_enum::enum_name(op->type)));
    }

    return std::move(lookup);
}

auto resolveSelectListNullability(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ZmqChannel& channel) -> NullableLookup {
    NullableLookup internal_join_types{};
    CteColumnBindings cte_columns{};

    return resolveSelectListNullabilityInternal(op, internal_join_types, cte_columns, channel);
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
    NullableLookup::Nullability nullable;
};

static auto runResolveSelectListNullability(const std::string& sql, std::vector<std::string>&& schemas, const std::vector<ColumnBindingPair>& expects) -> void {
        duckdb::DuckDB db(nullptr);
        duckdb::Connection conn(db);

        for (auto& schema: schemas) {
            conn.Query(schema);
        }

        auto stmts = conn.ExtractStatements(sql);

        NullableLookup join_type_result;
        try {
            conn.BeginTransaction();

            auto bound_result = bindTypeToStatement(*conn.context, std::move(stmts[0]->Copy()), {});
            auto channel = ZmqChannel::unitTestChannel();
            join_type_result = resolveSelectListNullability(bound_result.stmt.plan, channel);

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
        std::vector<NullableLookup::Column> bindings_result(view.begin(), view.end());

        for (int i = 0; i < expects.size(); ++i) {
            auto expect = expects[i];

            INFO(std::format("has column binding#{}", i+1));
            CHECK_THAT(bindings_result, VectorContains(NullableLookup::Column::from(expect.binding)));
            
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