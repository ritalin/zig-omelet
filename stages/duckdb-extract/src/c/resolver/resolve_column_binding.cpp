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

#include <iostream>

namespace worker {

// auto DumpNodes(std::vector<NodeRef>& nodes) -> void {
//     for (size_t i = 0; auto& node: nodes) {
//         auto n = node;
//         std::cout << std::format("[{}] ", i);
//         while (n) {
//             std::cout << std::format("({}, {}) -> ", n->table_index, n->column_index);
//             n = n->next;
//         } 
//         std::cout << "(null)" << std::endl << std::endl;;
//         ++i;
//     }
// }

// auto ColumnExpressionVisitor::RegisterBindingLink(const std::optional<duckdb::ColumnBinding>& current_binding, bool nullable) -> void {
//     this->nullable_stack.push(nullable);
// }

auto ColumnExpressionVisitor::VisitReplace(duckdb::BoundConstantExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    auto nullable = expr.value.IsNull();
    // std::cout << std::format("[IN VisitReplace/BoundConstantExpression] (current: {}, {})", current.value().table_index, current.value().column_index) << std::endl;
    
    // this->RegisterBindingLink(this->current_binding, nullable);
    this->nullable_stack.push(nullable);

    return nullptr;
}

auto ColumnExpressionVisitor::VisitReplace(duckdb::BoundFunctionExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    bool nullable = true;
    if (expr.is_operator) {
        nullable = this->EvalNullability(expr.children, true);
    }

    // this->RegisterBindingLink(this->current_binding, nullable);
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
    
    // this->RegisterBindingLink(this->current_binding, nullable);
    this->nullable_stack.push(nullable);

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto ColumnExpressionVisitor::VisitReplace(duckdb::BoundComparisonExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    switch (expr.type) {
    case duckdb::ExpressionType::COMPARE_DISTINCT_FROM: 
        // is [not] false/true
        // this->RegisterBindingLink(this->current_binding, false);
        this->nullable_stack.push(false);
        break;
    default: 
        duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> children;
        {
            children.push_back(expr.left->Copy());
            children.push_back(expr.right->Copy());
        }
        auto nullable = this->EvalNullability(children, true);
        // this->RegisterBindingLink(this->current_binding, nullable);
        this->nullable_stack.push(nullable);

        break;
    }

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto ColumnExpressionVisitor::VisitReplace(duckdb::BoundParameterExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    // this->RegisterBindingLink(this->current_binding, true);
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

    // this->RegisterBindingLink(this->current_binding, nullable);
    this->nullable_stack.push(nullable);

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto ColumnExpressionVisitor::VisitReplace(duckdb::BoundSubqueryExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    this->nullable_stack.push(true);

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

// auto ColumnExpressionVisitor::VisitReplace(duckdb::BoundColumnRefExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
//     auto current = this->current_binding;
    
//     auto table_index = expr.binding.table_index;
//     auto column_index = expr.binding.column_index;

//     duckdb::idx_t index = 0;
//     auto view = this->nodes 
//         | std::views::filter([&](const NodeRef& node) { return node->table_index == table_index; })
//         | std::views::filter([&](const NodeRef& node) { return index++ == column_index; })
//     ;
//     std::optional<NodeRef> next_node = view.begin() != view.end() ? std::optional{view.front()} : std::nullopt;

//     if (current) {
//         // std::cout 
//         // << std::format("[IN VisitReplace/BoundColumnRefExpression] current: ({}, {}) expr: ({}, {}) nodes: {}, nex: {}", 
//         //     current.value().table_index, current.value().column_index, table_index, column_index, this->nodes.size(), (bool)(view.begin() != view.end())) 
//         // << std::endl;

//         auto node = std::make_shared<ColumnBindingNode>(
//             nullptr,
//             NodeKind::Consume,
//             current.value().table_index,
//             current.value().column_index,
//             true
//         );

//         if (next_node) {
//             node->next = next_node.value();
//         }
        
//         this->nodes.push_back(node);

//         // DumpNodes(this->nodes);
//     }
//     else {
//         this->nullable_stack.push(next_node ? next_node.value()->nullable : true);
//     }

//     return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
// }

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
    // std::optional<duckdb::ColumnBinding> sv_binding = this->current_binding;
    // this->current_binding.reset();

    for (auto& expr: expressions) {
        if (this->EvalNullabilityInternal(expr) == terminate_value) {
            // this->current_binding = sv_binding;
            return terminate_value;
        }
    }

    // this->current_binding = sv_binding;
    return (! terminate_value);
}

// auto ColumnExpressionVisitor::VisitColumnBinding(duckdb::unique_ptr<duckdb::Expression>&& expr, duckdb::ColumnBinding&& binding) -> void {
//     this->current_binding = std::move(binding);
    
//     this->VisitExpression(&expr);
//     this->current_binding.reset();
// }

// static auto VisitOperatorGet(CatalogLookup catalogs, std::vector<NodeRef>& nodes, const duckdb::LogicalGet& op) -> void {
//     std::unordered_set<duckdb::idx_t> nn_columns{};

//     if (catalogs.contains(op.table_index)) {
//         auto& constraints = catalogs.at(op.table_index)->GetConstraints();
//         auto nn_view = constraints
//             | std::views::filter([](const duckdb::unique_ptr<duckdb::Constraint>& c) { return c->type == duckdb::ConstraintType::NOT_NULL; })
//             | std::views::transform([](const duckdb::unique_ptr<duckdb::Constraint>& c) { return c->Cast<duckdb::NotNullConstraint>().index.index; })
//         ;
//         nn_columns = std::move(std::unordered_set<duckdb::idx_t>(nn_view.begin(), nn_view.end()));
//     }

//     for (auto id: op.column_ids) {
//         nodes.push_back(std::make_shared<ColumnBindingNode>(
//             nullptr,
//             NodeKind::FromTable,
//             op.table_index,
//             id,
//             (! nn_columns.contains(id))
//         ));
//     }
// }

// auto ColumnExpressionVisitor::VisitOperator(duckdb::LogicalOperator &op) -> void {
//     if (op.type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
//         auto& op_projection = op.Cast<duckdb::LogicalProjection>();

//         this->VisitOperatorChildren(op);

//         for (duckdb::idx_t column_index = 0; auto& expr: op.expressions) {
//             this->VisitColumnBinding(expr->Copy(), duckdb::ColumnBinding(op_projection.table_index, column_index));
//             ++column_index;
//         }
//     }
//     else if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
//         VisitOperatorGet(this->catalogs, this->nodes, op.Cast<duckdb::LogicalGet>());
//     }
//     else {
//         duckdb::LogicalOperatorVisitor::VisitOperatorChildren(op);
//     }
// }

// --------

// auto convertToColumnBindingPairInternal(const NodeRef& parent_node, const NodeRef& node) -> NodeRef {
//     if (! node) {
//         return parent_node;
//     }

//     return convertToColumnBindingPairInternal(node, node->next);
// }

// auto convertToColumnBindingPair(std::vector<NodeRef>& nodes) -> std::vector<ColumnBindingPair> {
//     std::vector<ColumnBindingPair> result;
//     result.reserve(nodes.size());

//     for (auto& node: nodes) {
//         if (node->kind != NodeKind::FromTable) {
//             auto bottom = convertToColumnBindingPairInternal(node, node->next);
//             result.emplace_back(ColumnBindingPair{
//                 .binding = duckdb::ColumnBinding(node->table_index, node->column_index),
//                 .nullable = bottom->nullable,
//             });
//         }
//     }

//     return result;
// }

auto ColumnExpressionVisitor::Resolve(duckdb::unique_ptr<duckdb::Expression> &expr) -> NullableLookup::Nullability {
    ColumnExpressionVisitor visitor;

    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> exprs;
    {
        exprs.reserve(1);
        exprs.push_back(expr->Copy());
    }

    return std::move(NullableLookup::Nullability{
        .from_field = visitor.EvalNullability(exprs, true), 
        .from_join = false
    });
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

// using LogicalOperatorRef = duckdb::unique_ptr<duckdb::LogicalOperator>;
// using BoundTableRef = duckdb::unique_ptr<duckdb::BoundTableRef>;

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

        auto bound_statement = bindTypeToStatement(*conn.context, std::move(stmts[0]->Copy()));
        // auto bound_table_ref = bindTypeToTableRef(*conn.context, std::move(stmts[0]->Copy()), stmt_type);
        // auto catalogs = TableCatalogResolveVisitor::Resolve(bound_table_ref);

        for (duckdb::idx_t i = 0; auto& expr: bound_statement.plan->expressions) {
            NullableLookup::Column binding{ .table_index = 1, .column_index = i++ };
            results[binding] = ColumnExpressionVisitor::Resolve(expr);
        }
        conn.Commit();
    }
    catch (...) {
        conn.Rollback();
        throw;
    }

    result_size: {
        UNSCOPED_INFO("Result size");
        REQUIRE(results.size() == expects.size());
    }
    result_entries: {
        auto view = results | std::views::keys;

        std::vector<NullableLookup::Column> bindings_result(view.begin(), view.end());
        
        for (int i = 1; auto& expect: expects) {
            INFO(std::format("Result entry#{}", i));
            has_entry: {
                UNSCOPED_INFO("Has entry");
                REQUIRE_THAT(bindings_result, VectorContains(NullableLookup::Column::from(expect.binding)));
            }
            column_nullability: {
                UNSCOPED_INFO("Nullability");
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
