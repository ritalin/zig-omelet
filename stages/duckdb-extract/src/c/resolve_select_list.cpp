#include <duckdb.hpp>
#include <duckdb/planner/logical_operator_visitor.hpp>
#include <duckdb/planner/expression/list.hpp>
#include <duckdb/planner/tableref/list.hpp>

#include "duckdb_binder_support.hpp"

#include <iostream>
#include <magic_enum/magic_enum.hpp>

namespace worker {

class SelectListVisitor: public duckdb::LogicalOperatorVisitor {
private: 
    class DummyExpression: public duckdb::Expression {
    public:
        DummyExpression(): duckdb::Expression(duckdb::ExpressionType::INVALID, duckdb::ExpressionClass::INVALID, duckdb::LogicalType::SQLNULL) {}
        auto ToString() const -> std::string { return ""; }
        auto Copy() -> duckdb::unique_ptr<Expression> { return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression()); }
    };
public:
    SelectListVisitor(duckdb::unique_ptr<duckdb::BoundTableRef> table_ref): table_ref(std::move(table_ref)) {}
public:
    auto Visit(duckdb::unique_ptr<duckdb::LogicalOperator>& op) -> std::vector<ColumnEntry>;
    auto VisitNullability(duckdb::unique_ptr<duckdb::Expression>& expr) -> bool;
public:
    auto VisitExpressionChildren(duckdb::Expression& expr) -> void;
protected:
	auto VisitReplace(duckdb::BoundConstantExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
	auto VisitReplace(duckdb::BoundComparisonExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
	auto VisitReplace(duckdb::BoundOperatorExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundFunctionExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;

private:
    auto VisitNullabilityInternal(duckdb::unique_ptr<duckdb::Expression>& expr) -> bool;
private:
    duckdb::unique_ptr<duckdb::BoundTableRef> table_ref;
    std::stack<bool> nullable_result;
};

auto SelectListVisitor::VisitExpressionChildren(duckdb::Expression& expr) -> void {
    if (dynamic_cast<DummyExpression*>(&expr) != nullptr) {
        return;
    }
    else {
        duckdb::LogicalOperatorVisitor::VisitExpressionChildren(expr);
    }
}

auto SelectListVisitor::VisitReplace(duckdb::BoundConstantExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    this->nullable_result.push(expr.value.IsNull());

    return nullptr;
}

auto SelectListVisitor::VisitReplace(duckdb::BoundComparisonExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    switch (expr.type) {
    case duckdb::ExpressionType::COMPARE_DISTINCT_FROM: 
        // is [not] false/true
        this->nullable_result.push(false);
        break;
    default: 
        this->nullable_result.push(this->VisitNullabilityInternal(expr.left) | this->VisitNullabilityInternal(expr.right));
        break;
    }

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto SelectListVisitor::VisitReplace(duckdb::BoundOperatorExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    auto nullable = false;

    switch (expr.type) {
    case duckdb::ExpressionType::OPERATOR_IS_NULL:
    case duckdb::ExpressionType::OPERATOR_IS_NOT_NULL:
        // always NOT null
        break;
    case duckdb::ExpressionType::OPERATOR_COALESCE:
    case duckdb::ExpressionType::OPERATOR_NULLIF:
        nullable = true;
        for (auto& child: expr.children) {
            if (! this->VisitNullabilityInternal(child)) {
                nullable = false;
                break;
            }
        }
        break;
    default: 
        for (auto& child: expr.children) {
            if (this->VisitNullabilityInternal(child)) {
                nullable = true;
                break;
            }
        }
        break;
    }
    
    this->nullable_result.push(nullable);

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto SelectListVisitor::VisitReplace(duckdb::BoundFunctionExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    if (expr.is_operator) {
        for (auto& child: expr.children) {
            if (this->VisitNullabilityInternal(child)) {
                this->nullable_result.push(true);
                break;
            }
        }
    }
    else {
        this->nullable_result.push(true);
    }

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto SelectListVisitor::VisitNullabilityInternal(duckdb::unique_ptr<duckdb::Expression>& expr) -> bool {
    size_t depth = this->nullable_result.size();

    this->VisitExpression(&expr);

    auto result = false;
    if (this->nullable_result.size() > depth) {
        result = this->nullable_result.top();
        this->nullable_result.pop();
    }

    return result;
}

auto SelectListVisitor::VisitNullability(duckdb::unique_ptr<duckdb::Expression>& expr) -> bool {
    this->nullable_result = {};
    return this->VisitNullabilityInternal(expr);
}

auto SelectListVisitor::Visit(duckdb::unique_ptr<duckdb::LogicalOperator>& op) -> std::vector<ColumnEntry> {
    std::cout << std::format("op/type: {}", magic_enum::enum_name(op->type)) << std::endl;

    std::vector<ColumnEntry> result;
    result.reserve(op->expressions.size());

    for (size_t i = 0; auto& expr: op->expressions) {
        std::cout << std::format("Expression/name: {}, class: {}", expr->alias, magic_enum::enum_name(expr->expression_class)) << std::endl;
        std::cout << std::format("Expression/type: {}", magic_enum::enum_name(expr->type)) << std::endl;

        auto entry = ColumnEntry{
            .field_name = expr->alias,
            .field_type = expr->return_type.ToString(),
            .nullable = this->VisitNullability(expr),
        };
        std::cout << std::format("Expression/nullable: {}", entry.nullable) << std::endl;
        result.emplace_back(std::move(entry));
        std::cout << std::format("") << std::endl;
    }
    std::cout << std::format("") << std::endl;


    return std::move(result);
}

auto resolveColumnType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, duckdb::unique_ptr<duckdb::BoundTableRef>&& table_ref, StatementType stmt_type) -> std::vector<ColumnEntry> {
    if (stmt_type != StatementType::Select) return {};

    SelectListVisitor visitor(std::move(table_ref));
    return visitor.Visit(op);
}

}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <utility>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

using namespace worker;
using namespace Catch::Matchers;

using LogicalOperatorRef = duckdb::unique_ptr<duckdb::LogicalOperator>;
using BoundTableRef = duckdb::unique_ptr<duckdb::BoundTableRef>;

static auto runBindTypeToStatement(duckdb::Connection& conn, duckdb::unique_ptr<duckdb::SQLStatement>&& stmt) -> LogicalOperatorRef {
    duckdb::BoundStatement bind_result;
    try {
        conn.BeginTransaction();
        bind_result = bindTypeToStatement(*conn.context, std::forward<duckdb::unique_ptr<duckdb::SQLStatement>>(stmt));
        conn.Commit();
    }
    catch (...) {
        conn.Rollback();
        throw;
    }

    return std::move(bind_result.plan);
}

static auto runBindTypeToTableRef(duckdb::Connection& conn, duckdb::unique_ptr<duckdb::SQLStatement>&& stmt, StatementType stmt_type) -> BoundTableRef {
    duckdb::unique_ptr<duckdb::BoundTableRef> bind_result;
    try {
        conn.BeginTransaction();
        bind_result = bindTypeToTableRef(*conn.context, std::forward<duckdb::unique_ptr<duckdb::SQLStatement>>(stmt), stmt_type);
        conn.Commit();
    }
    catch (...) {
        conn.Rollback();
        throw;
    }

    return std::move(bind_result);
}

static auto runBindStatement(const std::string sql, const std::vector<std::string>& schemas) -> std::tuple<StatementType, LogicalOperatorRef, BoundTableRef> {
    auto db = duckdb::DuckDB(nullptr);
    auto conn = duckdb::Connection(db);

    prepare_schema: {
        for (auto& schema: schemas) {
            conn.Query(schema);
        }
    }

    auto stmts = conn.ExtractStatements(sql);
    auto stmt_type = evalStatementType(stmts[0]);

    return std::make_tuple<StatementType, LogicalOperatorRef, BoundTableRef>(
        std::move(stmt_type),
        runBindTypeToStatement(conn, std::move(stmts[0]->Copy())), 
        runBindTypeToTableRef(conn, std::move(stmts[0]->Copy()), stmt_type)
    );
}

static auto runResolveColumnTypeForSelectStatement(LogicalOperatorRef& op, BoundTableRef&& table_ref, StatementType stmt_type, const std::vector<ColumnEntry>& expected) -> void {
    auto column_result = resolveColumnType(op, std::forward<BoundTableRef>(table_ref), stmt_type);

    SECTION("Result size") {
        REQUIRE(column_result.size() == expected.size());
    }
    SECTION("Result entries") {
        for (int i = 0; auto& entry: column_result) {
            SECTION(std::format("entry#{}", i+1)) {
                SECTION("field name") {
                    CHECK_THAT(entry.field_name, Equals(expected[i].field_name));
                }
                SECTION("field type") {
                    CHECK_THAT(entry.field_type, Equals(expected[i].field_type));
                }
                SECTION("nullable") {
                    CHECK(entry.nullable == expected[i].nullable);
                }
            }
            ++i;
        }
    }
}

static auto runResolveColumnTypeForOtherStatement(LogicalOperatorRef& op, BoundTableRef&& table_ref, StatementType stmt_type) -> void {
    auto column_result = resolveColumnType(op, std::move(table_ref), stmt_type);

    SECTION("Resolve result (NOT generate select list)") {
        REQUIRE(column_result.size() == 0);
    }
}

TEST_CASE("Insert Statement") {
    std::string sql("insert into Foo values (42, 1, null, 'misc...')");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    auto [stmt_type, op, table_ref] = runBindStatement(sql, {schema});
    runResolveColumnTypeForOtherStatement(op, std::move(table_ref), stmt_type);
}

TEST_CASE("Update Statement") {
    std::string sql("update Foo set kind = 2, xys = 101 where id = 42");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    auto [stmt_type, op, table_ref] = runBindStatement(sql, {schema});
    runResolveColumnTypeForOtherStatement(op, std::move(table_ref), stmt_type);
}

TEST_CASE("Delete Statement") {
    std::string sql("delete from Foo where id = 42");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    auto [stmt_type, op, table_ref] = runBindStatement(sql, {schema});
    runResolveColumnTypeForOtherStatement(op, std::move(table_ref), stmt_type);  
}

TEST_CASE("Select list only") {
    std::string sql("select 123 as a, 98765432100 as b, 'abc' as c");
    std::vector<ColumnEntry> expected{
        {.field_name = "a", .field_type = "INTEGER", .nullable = false},
        {.field_name = "b", .field_type = "BIGINT", .nullable = false},
        {.field_name = "c", .field_type = "VARCHAR", .nullable = false},
    };

    auto [stmt_type, op, table_ref] = runBindStatement(sql, {});
    runResolveColumnTypeForSelectStatement(op, std::move(table_ref), stmt_type, expected);
}

TEST_CASE("Select list only with null#1") {
    std::string sql("select 123 as a, 98765432100 as b, null::date as c");
    std::vector<ColumnEntry> expected{
        {.field_name = "a", .field_type = "INTEGER", .nullable = false},
        {.field_name = "b", .field_type = "BIGINT", .nullable = false},
        {.field_name = "c", .field_type = "DATE", .nullable = true},
    };

    std::cout << std::format("Select list only with null#1") << std::endl;
    auto [stmt_type, op, table_ref] = runBindStatement(sql, {});
    runResolveColumnTypeForSelectStatement(op, std::move(table_ref), stmt_type, expected);
}

TEST_CASE("Select list only with null#2") {
    std::string sql("select 123 + null as a, 98765432100 as b, 'abc' || null as c");
    std::vector<ColumnEntry> expected{
        {.field_name = "a", .field_type = "INTEGER", .nullable = true},
        {.field_name = "b", .field_type = "BIGINT", .nullable = false},
        {.field_name = "c", .field_type = "VARCHAR", .nullable = true},
    };

    std::cout << std::format("Select list only with null#2") << std::endl;
    auto [stmt_type, op, table_ref] = runBindStatement(sql, {});
    runResolveColumnTypeForSelectStatement(op, std::move(table_ref), stmt_type, expected);
}

TEST_CASE("Select list only with null#3") {
    std::string sql("select (null) is not false as a, null is null as b, null is not null as c");
    std::vector<ColumnEntry> expected{
        {.field_name = "a", .field_type = "BOOLEAN", .nullable = false},
        {.field_name = "b", .field_type = "BOOLEAN", .nullable = false},
        {.field_name = "c", .field_type = "BOOLEAN", .nullable = false},
    };

    std::cout << std::format("Select list only with null#3") << std::endl;
    auto [stmt_type, op, table_ref] = runBindStatement(sql, {});
    runResolveColumnTypeForSelectStatement(op, std::move(table_ref), stmt_type, expected);
}

TEST_CASE("Select list only without alias") {
    std::string sql("select 123, select 'abc'");
}

TEST_CASE("Select list only with parameter without alias") {
    std::string sql(R"#(
        select 123::bigint, 123, select $1::int as "CAST($seq AS INTEGER)"
    )#");
}

TEST_CASE("Select list of star") {
    std::string sql("select * from Foo");

}

#endif