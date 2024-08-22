#include <ranges>
#include <iostream>

#include <duckdb.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_delim_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_comparison_join.hpp>
#include <duckdb/planner/operator/logical_any_join.hpp>
#include <duckdb/planner/bound_tableref.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>

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

    for (duckdb::idx_t i = 0; auto id: op.column_ids) {
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

auto JoinTypeVisitor::VisitOperatorCondition(duckdb::LogicalOperator &op, duckdb::JoinType join_type, const ConditionRels& rels) -> NullableLookup {
    NullableLookup internal_join_types{};
    JoinTypeVisitor visitor(internal_join_types, this->channel);

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
            }
            else {
                duckdb::LogicalOperatorVisitor::VisitOperator(op);
            }
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

static auto VisitOperatorProjection(duckdb::LogicalProjection& op, ZmqChannel& channel) -> NullableLookup {
    NullableLookup internal_join_types{};
    JoinTypeVisitor visitor(internal_join_types, channel);

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
            auto lookup = VisitOperatorProjection(op.Cast<duckdb::LogicalProjection>(), this->channel);
            this->join_type_lookup.insert(lookup.begin(), lookup.end());
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_GET:
        this->VisitOperatorGet(op.Cast<duckdb::LogicalGet>());
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

auto resolveSelectListNullability(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ZmqChannel& channel) -> NullableLookup {
    NullableLookup lookup;

    if (op->type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
        lookup = VisitOperatorProjection(op->Cast<duckdb::LogicalProjection>(), channel);
    }

    return std::move(lookup);
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
#include <magic_enum/magic_enum.hpp>

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

            auto bound_statement = bindTypeToStatement(*conn.context, std::move(stmts[0]->Copy()));
            auto channel = ZmqChannel::unitTestChannel();
            join_type_result = resolveSelectListNullability(bound_statement.plan, channel);

            conn.Commit();
        }
        catch (...) {
            conn.Rollback();
            throw;
        }

    Result_size: {
        UNSCOPED_INFO("Result size");
        REQUIRE(join_type_result.size() == expects.size());
    }
    Result_items: {    
        auto view = join_type_result | std::views::keys;
        std::vector<NullableLookup::Column> bindings_result(view.begin(), view.end());

        for (int i = 0; i < expects.size(); ++i) {
            auto expect = expects[i];

            INFO(std::format("Result item#{}", i+1)); 

            UNSCOPED_INFO("has column binding");
            CHECK_THAT(bindings_result, VectorContains(NullableLookup::Column::from(expect.binding)));
            
            UNSCOPED_INFO("column nullability (field)");
            CHECK(join_type_result[expect.binding].from_field == expect.nullable.from_field);
            
            UNSCOPED_INFO("column nullability (join)");
            CHECK(join_type_result[expect.binding].from_join == expect.nullable.from_join);
        }
    }
}

TEST_CASE("fromless query") {
    std::string sql(R"#(
        select 123, 'abc'
    )#");

    std::vector<ColumnBindingPair> expects{
        { .binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false} },
        { .binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = false, .from_join = false} },
    };

    runResolveSelectListNullability(sql, {}, expects);
}

TEST_CASE("joinless query#1") {
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

TEST_CASE("joinless query#2 (unordered select list)") {
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

TEST_CASE("joinless query#3 (with unary op of nallble)") {
    std::string sql("select -xys from Foo");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = true, .from_join = false}},
    };

    runResolveSelectListNullability(sql, {schema}, expects);
}

TEST_CASE("Inner join#1") {
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

TEST_CASE("Inner join#2 (nullable key)") {
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

TEST_CASE("Inner join#3 (join twice)") {
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

TEST_CASE("Left outer join") {
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

TEST_CASE("Right outer join") {
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

TEST_CASE("Left outer join twice") {
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

TEST_CASE("Inner join + outer join") {
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

TEST_CASE("Outer join + inner join#1") {
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

TEST_CASE("Outer join + inner join#2") {
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

TEST_CASE("scalar subquery (single left outer join)") {
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

TEST_CASE("Cross join") {
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

TEST_CASE("Cross join + outer join") {
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

TEST_CASE("Full outer join") {
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

TEST_CASE("Inner join + full outer join#1") {
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

TEST_CASE("Inner join + full outer join#2") {
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

TEST_CASE("Full outer + inner join join#1") {
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

TEST_CASE("Full outer + inner join join#2") {
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

TEST_CASE("Positional join") {
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

TEST_CASE("Inner join lateral#1") {
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

TEST_CASE("Inner join lateral#2 (unordered select list)") {
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

TEST_CASE("Inner join lateral#3 (nullable key)") {
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

TEST_CASE("Outer join lateral") {
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

TEST_CASE("Inner join with subquery#1") {
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

TEST_CASE("Inner join with subquery#2") {
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

#endif