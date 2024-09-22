#include <algorithm>
#include <ranges>

#include <duckdb.hpp>
#include <duckdb/planner/expression/list.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/planner/bound_tableref.hpp>

#include "duckdb_logical_visitors.hpp"
#include "duckdb_nullable_lookup.hpp"

#include <iostream>
#include <numeric>

namespace worker {

auto ColumnExpressionVisitor::VisitReplace(duckdb::BoundConstantExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    auto nullable = expr.value.IsNull();
    
    this->nullable_stack.push(nullable);

    return nullptr;
}

static std::unordered_set<std::string> child_extractor{"struct_extract"};

static auto extractFieldPath(duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& args) -> std::string {
    auto name_view = args | std::views::transform([](duckdb::unique_ptr<duckdb::Expression>& arg) {
        if (arg->expression_class == duckdb::ExpressionClass::BOUND_CONSTANT) {
            return arg->Cast<duckdb::BoundConstantExpression>().value.ToString();
        }
        else {
            return arg->alias;
        }
    });

    return std::accumulate(name_view.begin()+1, name_view.end(), name_view.front(), [](auto acc, const auto p) {
        return std::format("{}.{}", acc, p);
    });
}

static auto extractFieldNullable(duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& args, SampleNullableCache& cache) -> bool {
    if (args.size() == 0) return true;
    if (args[0]->expression_class != duckdb::ExpressionClass::BOUND_COLUMN_REF) return true;

    auto table_index = args[0]->Cast<duckdb::BoundColumnRefExpression>().binding.table_index;
    auto& sample_map = cache[table_index];

    auto path = extractFieldPath(args);
    return sample_map.contains(path) ? sample_map[path].from_field : true;
}

auto ColumnExpressionVisitor::VisitReplace(duckdb::BoundFunctionExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    bool nullable = true;
    if (expr.is_operator) {
        nullable = this->EvalNullability(expr.children, true);
    }
    else {
        if (child_extractor.contains(expr.function.name)) {
            // apply `unnest` function
            nullable = extractFieldNullable(expr.children, this->sample_cache);
        }
    }

    this->nullable_stack.push(nullable);

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto ColumnExpressionVisitor::VisitReplace(duckdb::BoundAggregateExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    bool nullable = true;
    this->nullable_stack.push(nullable);

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto ColumnExpressionVisitor::VisitReplace(duckdb::BoundWindowExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    bool nullable = true;
    this->nullable_stack.push(nullable);

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto ColumnExpressionVisitor::VisitReplace(duckdb::BoundUnnestExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    bool nullable = true;

    if (expr.child->expression_class == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
        auto& child_expr = expr.child->Cast<duckdb::BoundColumnRefExpression>();
        if (child_expr.return_type.id() == duckdb::LogicalTypeId::LIST) {
            auto& cache = this->sample_cache[child_expr.binding.table_index];
            auto name = child_expr.alias + ".";

            nullable = cache.contains(name) ? cache[name].from_field : true;
        }
    }
    this->nullable_stack.push(nullable);

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto ColumnExpressionVisitor::VisitReplace(duckdb::BoundOperatorExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    auto nullable = false;

    switch (expr.type) {
    case duckdb::ExpressionType::OPERATOR_IS_NULL:
    case duckdb::ExpressionType::OPERATOR_IS_NOT_NULL:
        // always NOT null
        break;
    case duckdb::ExpressionType::OPERATOR_COALESCE:
    case duckdb::ExpressionType::OPERATOR_NULLIF:
        nullable = this->EvalNullability(expr.children, false);
        break;
    default: 
        nullable = this->EvalNullability(expr.children, true);
        break;
    }
    
    this->nullable_stack.push(nullable);

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto ColumnExpressionVisitor::VisitReplace(duckdb::BoundComparisonExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    switch (expr.type) {
    case duckdb::ExpressionType::COMPARE_DISTINCT_FROM: 
        // is [not] false/true
        this->nullable_stack.push(false);
        break;
    default: 
        duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> children;
        {
            children.push_back(expr.left->Copy());
            children.push_back(expr.right->Copy());
        }
        auto nullable = this->EvalNullability(children, true);
        this->nullable_stack.push(nullable);

        break;
    }

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto ColumnExpressionVisitor::VisitReplace(duckdb::BoundParameterExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    this->nullable_stack.push(true);
    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto ColumnExpressionVisitor::VisitReplace(duckdb::BoundCaseExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    auto nullable = true;

    if (expr.else_expr) {
        duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> children;
        children.push_back(expr.else_expr->Copy());
        nullable = this->EvalNullability(children, true);
    }

    this->nullable_stack.push(nullable);

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto ColumnExpressionVisitor::VisitReplace(duckdb::BoundSubqueryExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    this->nullable_stack.push(true);

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto ColumnExpressionVisitor::VisitReplace(duckdb::BoundColumnRefExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    auto binding = NullableLookup::Column::from(expr.binding);
    this->nullable_stack.push(this->nullabilities[binding].from_field);

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto ColumnExpressionVisitor::EvalNullabilityInternal(duckdb::unique_ptr<duckdb::Expression>& expr) -> bool {
    size_t depth = this->nullable_stack.size();

    this->VisitExpression(&expr);

    auto result = false;
    if (this->nullable_stack.size() > depth) {
        result = this->nullable_stack.top();
        this->nullable_stack.pop();
    }

    while (this->nullable_stack.size() > depth) {
        this->nullable_stack.pop();
    }

    return result;
}

auto ColumnExpressionVisitor::EvalNullability(duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& expressions, bool terminate_value) -> bool {
    for (auto& expr: expressions) {
        if (this->EvalNullabilityInternal(expr) == terminate_value) {
            return terminate_value;
        }
    }

    return (! terminate_value);
}

auto ColumnExpressionVisitor::Resolve(
    duckdb::unique_ptr<duckdb::Expression> &expr, 
    NullableLookup& nullabilities, 
    SampleNullableCache& sample_cache) -> NullableLookup::Nullability 
{
    ColumnExpressionVisitor visitor(nullabilities, sample_cache);

    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> exprs;
    {
        exprs.reserve(1);
        exprs.push_back(expr->Copy());
    }

    NullableLookup::Nullability result{};
    if (expr->expression_class == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
        auto& column_expr = expr->Cast<duckdb::BoundColumnRefExpression>();
        auto binding = NullableLookup::Column::from(column_expr.binding);
        result = nullabilities[binding];
    }

    result.from_field = visitor.EvalNullability(exprs, true);

    return result;
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

auto runCreateColumnBindingLookup(const std::string sql, const std::vector<std::string>& schemas, const std::vector<ColumnBindingPair>& expects) -> void {
    auto db = duckdb::DuckDB(nullptr);
    auto conn = duckdb::Connection(db);

    prepare_schema: {
        for (auto& schema: schemas) {
            conn.Query(schema);
        }
    }

    NullableLookup results;
    try {
        conn.BeginTransaction();

        auto stmts = conn.ExtractStatements(sql);
        auto stmt_type = evalStatementType(stmts[0]);

        auto bound_result = bindTypeToStatement(*conn.context, std::move(stmts[0]->Copy()), {}, {});

        NullableLookup field_lookup;
        SampleNullableCache sample_cache{};
        
        for (duckdb::idx_t i = 0; auto& expr: bound_result.stmt.plan->expressions) {
            NullableLookup::Column binding{ .table_index = 1, .column_index = i++ };
            results[binding] = ColumnExpressionVisitor::Resolve(expr, field_lookup, sample_cache);
        }
        conn.Commit();
    }
    catch (...) {
        conn.Rollback();
        throw;
    }

    result_size: {
        INFO("Result size");
        REQUIRE(results.size() == expects.size());
    }
    result_entries: {
        auto view = results | std::views::keys;

        std::vector<NullableLookup::Column> bindings_result(view.begin(), view.end());
        
        for (int i = 1; auto& expect: expects) {
            INFO(std::format("Result entry#{}", i));
            has_entry: {
                INFO("Has entry");
                REQUIRE_THAT(bindings_result, VectorContains(NullableLookup::Column::from(expect.binding)));
            }
            column_nullability: {
                INFO("Nullability");
                CHECK(results[expect.binding].from_field == expect.nullable.from_field);
            }
            ++i;
        }
    }
}

TEST_CASE("ColumnBinding#1") {
    std::string sql("select 123 as a, 98765432100 as b, 'abc' as c");
    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
        {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = false, .from_join = false}},
        {.binding = duckdb::ColumnBinding(1, 2), .nullable = {.from_field = false, .from_join = false}},
    };

    runCreateColumnBindingLookup(sql, {}, expects);
}

TEST_CASE("ColumnBinding#2 (with null)") {
    std::string sql("select 123 as a, 98765432100 as b, null::date as c");
    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
        {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = false, .from_join = false}},
        {.binding = duckdb::ColumnBinding(1, 2), .nullable = {.from_field = true, .from_join = false}},
    };

    runCreateColumnBindingLookup(sql, {}, expects);
}

TEST_CASE("ColumnBinding#3 (with null expr)") {
    std::string sql("select 123 + null as a, 98765432100 as b, 'abc' || null as c");
    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = true, .from_join = false}},
        {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = false, .from_join = false}},
        {.binding = duckdb::ColumnBinding(1, 2), .nullable = {.from_field = true, .from_join = false}},
    };

    runCreateColumnBindingLookup(sql, {}, expects);
}

TEST_CASE("ColumnBinding#4 (with `is` null operator)") {
    std::string sql("select (null) is not false as a, null is null as b, null is not null as c");
    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
        {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = false, .from_join = false}},
        {.binding = duckdb::ColumnBinding(1, 2), .nullable = {.from_field = false, .from_join = false}},
    };

    runCreateColumnBindingLookup(sql, {}, expects);
}

TEST_CASE("ColumnBinding#5 (with last not null coalesce)") {
    std::string sql("select coalesce(null, null, 10) as a");
    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
    };

    runCreateColumnBindingLookup(sql, {}, expects);
}

TEST_CASE("ColumnBinding#6 (with 2nd not null coalesce)") {
    std::string sql("select coalesce(null, 42, null) as a");
    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
    };

    runCreateColumnBindingLookup(sql, {}, expects);
}

TEST_CASE("ColumnBinding#7 (with null coalesce)") {
    std::string sql("select coalesce(null, null, null)::VARCHAR as a");
    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = true, .from_join = false}},
    };

    runCreateColumnBindingLookup(sql, {}, expects);
}

TEST_CASE("ColumnBinding#8 (with unary op)") {
    std::string sql("select -42 as a");
    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
    };

    runCreateColumnBindingLookup(sql, {}, expects);
}

TEST_CASE("ColumnBinding#9 (function call)") {
    std::string sql("select concat('hello ', 'world ') as fn");
    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = true, .from_join = false}},
    };

    runCreateColumnBindingLookup(sql, {}, expects);
}

TEST_CASE("ColumnBinding#10 (with parameter)") {
    auto sql = std::string(R"#(SELECT CAST($1 AS INTEGER) AS a, CAST($2 AS VARCHAR) AS "CAST($v AS VARCHAR)")#");

    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = true, .from_join = false}},
        {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false}},
    };

    runCreateColumnBindingLookup(sql, {}, expects);
}

TEST_CASE("ColumnBinding#11 (with unary op of NULL)") {
    std::string sql("select -(null) as a");
    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = true, .from_join = false}},
    };

    runCreateColumnBindingLookup(sql, {}, expects);
}

TEST_CASE("ColumnBinding#12 (with case expr#1)") {
    std::string sql(R"#(
        select (case when $1::int > 0 then 101 else 42 end) as xyz
    )#");

    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
    };

    runCreateColumnBindingLookup(sql, {}, expects);
}

TEST_CASE("ColumnBinding#12 (with case expr#2 - nullable else)") {
    std::string sql(R"#(
        select (case when $1::int > 0 then 101 else 42 end) as xyz
    )#");

    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
    };

    runCreateColumnBindingLookup(sql, {}, expects);
}

TEST_CASE("ColumnBinding#6 (with case expr#3 - without else)") {
    std::string sql(R"#(
        select (case when $1::int > 0 then 101 end) as xyz
    )#");

    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = true, .from_join = false}},
    };

    runCreateColumnBindingLookup(sql, {}, expects);
}

TEST_CASE("ColumnBinding#6 (with case expr#4 - nested)") {
    std::string sql(R"#(
        select 
            (case 
                when $1::int > 0 then 
                    case 
                        when $2::int < 100 then 100
                        when $2::int < 1000 then 1000
                        else 10000
                    end
                else -9999
            end) as xyz
    )#");

    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
    };

    runCreateColumnBindingLookup(sql, {}, expects);
}

TEST_CASE("ColumnBinding#7 (with binary comparison#1)") {
    std::string sql(R"#(
        select 
            123 < 456 as a, 
            987 > 654 as b, 
            42 = 42 as c,
            42 <> 101 as d,
            1 >= 2 as e,
            a <= 2 as f,
    )#");

    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
        {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = false, .from_join = false}},
        {.binding = duckdb::ColumnBinding(1, 2), .nullable = {.from_field = false, .from_join = false}},
        {.binding = duckdb::ColumnBinding(1, 3), .nullable = {.from_field = false, .from_join = false}},
        {.binding = duckdb::ColumnBinding(1, 4), .nullable = {.from_field = false, .from_join = false}},
        {.binding = duckdb::ColumnBinding(1, 5), .nullable = {.from_field = false, .from_join = false}},
    };

    runCreateColumnBindingLookup(sql, {}, expects);
}

TEST_CASE("ColumnBinding#7 (with binary comparison to null)") {
    std::string sql(R"#(
        select 
            123 < null as a, 
            987 > null as b, 
            42 = null as c,
            42 <> null as d,
            1 >= null as e,
            a <= null as f,
    )#");

    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = true, .from_join = false}},
        {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false}},
        {.binding = duckdb::ColumnBinding(1, 2), .nullable = {.from_field = true, .from_join = false}},
        {.binding = duckdb::ColumnBinding(1, 3), .nullable = {.from_field = true, .from_join = false}},
        {.binding = duckdb::ColumnBinding(1, 4), .nullable = {.from_field = true, .from_join = false}},
        {.binding = duckdb::ColumnBinding(1, 5), .nullable = {.from_field = true, .from_join = false}},
    };

    runCreateColumnBindingLookup(sql, {}, expects);
}

TEST_CASE("ColumnBinding#8 (with between comparison)") {
    std::string sql(R"#(
        select 42 between 0 and 101 as a
    )#");

    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
    };

    runCreateColumnBindingLookup(sql, {}, expects);
}

TEST_CASE("ColumnBinding#8 (with between comparison to null)") {
    std::string sql(R"#(
        select 42 between 0 and null as a
    )#");

    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = true, .from_join = false}},
    };

    runCreateColumnBindingLookup(sql, {}, expects);
}

TEST_CASE("ColumnBinding#9 (with scalar function call)") {
    std::string sql(R"#(
        select range(1, 10) as r
    )#");

    std::vector<ColumnBindingPair> expects{
        {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = true, .from_join = false}},
    };

    runCreateColumnBindingLookup(sql, {}, expects);
}

TEST_CASE("ColumnBinding#10 (with scalar subquery)") {
    SKIP("Scalar subquery is optimized to cross product or left outer join...");
}

#endif
