
#include <duckdb/parser/tableref/list.hpp>
#include <duckdb/parser/expression/list.hpp>

#define MAGIC_ENUM_RANGE_MAX (std::numeric_limits<uint8_t>::max())

#include <magic_enum/magic_enum.hpp>


#include "duckdb_binder_support.hpp"


static auto keepColumnName(duckdb::unique_ptr<duckdb::ParsedExpression>& expr) -> std::string {
    if (expr->alias != "") {
        return expr->alias;
    }
    else {
        return expr->ToString();
    }
}

static auto walkOrderBysNodeInternal(duckdb::unique_ptr<duckdb::OrderModifier>& order_bys, uint32_t depth, ZmqChannel& zmq_channel) -> void;
static auto walkSelectStatementInternal(duckdb::SelectStatement& stmt, uint32_t depth, ZmqChannel& zmq_channel) -> void;

static auto walkExpressionInternal(duckdb::unique_ptr<duckdb::ParsedExpression>& expr, uint32_t depth, ZmqChannel& zmq_channel) -> void {
    switch (expr->expression_class) {
    case duckdb::ExpressionClass::PARAMETER:
        {
            auto& param_expr = expr->Cast<duckdb::ParameterExpression>();
            param_expr.identifier = "1";
        }
        break;
    case duckdb::ExpressionClass::CAST: 
        {
            auto& cast_expr = expr->Cast<duckdb::CastExpression>();
            ::walkExpressionInternal(cast_expr.child, depth+1, zmq_channel);
        }
        break;
    case duckdb::ExpressionClass::COMPARISON:
        {
            auto& cmp_expr = expr->Cast<duckdb::ComparisonExpression>();
            ::walkExpressionInternal(cmp_expr.left, depth+1, zmq_channel);
            ::walkExpressionInternal(cmp_expr.right, depth+1, zmq_channel);
        }
        break;
    case duckdb::ExpressionClass::BETWEEN:
        {
            auto& between_expr = expr->Cast<duckdb::BetweenExpression>();
            ::walkExpressionInternal(between_expr.input, depth+1, zmq_channel);
            ::walkExpressionInternal(between_expr.lower, depth+1, zmq_channel);
            ::walkExpressionInternal(between_expr.upper, depth+1, zmq_channel);
        }
        break;
    case duckdb::ExpressionClass::CASE:
        {
            auto& case_expr = expr->Cast<duckdb::CaseExpression>();

            // whrn-then clause
            for (auto& case_check: case_expr.case_checks) {
                ::walkExpressionInternal(case_check.when_expr, depth+1, zmq_channel);
                ::walkExpressionInternal(case_check.then_expr, depth+1, zmq_channel);
            }
            
            // else clause
            ::walkExpressionInternal(case_expr.else_expr, depth+1, zmq_channel);
        }
        break;
    case duckdb::ExpressionClass::CONJUNCTION:
        {
            auto& conj_expr = expr->Cast<duckdb::ConjunctionExpression>();
            
            for (auto& child: conj_expr.children) {
                ::walkExpressionInternal(child, depth+1, zmq_channel);
            }
        }
        break;
    case duckdb::ExpressionClass::FUNCTION:
        {
            auto& fn_expr = expr->Cast<duckdb::FunctionExpression>();

            for (auto& child: fn_expr.children) {
                ::walkExpressionInternal(child, depth+1, zmq_channel);
            }

            // order by(s)
            ::walkOrderBysNodeInternal(fn_expr.order_bys, depth+1, zmq_channel);
        }
        break;
    case duckdb::ExpressionClass::SUBQUERY:
        {
            auto& sq_expr = expr->Cast<duckdb::SubqueryExpression>();
            ::walkSelectStatementInternal(*sq_expr.subquery, depth+1, zmq_channel);

            if (sq_expr.subquery_type == duckdb::SubqueryType::ANY) {
                ::walkExpressionInternal(sq_expr.child, depth+1, zmq_channel);
            }
        }
        break;
    case duckdb::ExpressionClass::CONSTANT:
    case duckdb::ExpressionClass::COLUMN_REF:
        // no conversion
        break;
    case duckdb::ExpressionClass::OPERATOR:
    default: 
        zmq_channel.warn(std::format("[TODO] Unsupported expression class: {} (depth: {})", magic_enum::enum_name(expr->expression_class), depth));
        break;
    }
}

static auto walkOrderBysNodeInternal(duckdb::unique_ptr<duckdb::OrderModifier>& order_bys, uint32_t depth, ZmqChannel& zmq_channel) -> void {
    for (auto& order_by: order_bys->orders) {
        ::walkExpressionInternal(order_by.expression, depth, zmq_channel);
    }
}

static auto walkSelectListItem(duckdb::unique_ptr<duckdb::ParsedExpression>& expr, uint32_t depth, ZmqChannel& zmq_channel) -> void {
    if (expr->HasParameter() || expr->HasSubquery()) {
        if (depth > 0) {
            ::walkExpressionInternal(expr, depth, zmq_channel);
        }
        else {
            auto new_alias = keepColumnName(expr);

            ::walkExpressionInternal(expr, depth, zmq_channel);

            if (expr->ToString() != new_alias) {
                expr->alias = new_alias;
            }

            // TODO: record untyped patameter column (untyped == no type casting)

        }
    }
}

static auto walkTableRef(duckdb::unique_ptr<duckdb::TableRef>& table_ref, uint32_t depth, ZmqChannel& zmq_channel) -> void {
    switch (table_ref->type) {
    case duckdb::TableReferenceType::BASE_TABLE:
    case duckdb::TableReferenceType::EMPTY_FROM:
        // no conversion
        break;
    case duckdb::TableReferenceType::TABLE_FUNCTION:
        {
            auto& table_fn = table_ref->Cast<duckdb::TableFunctionRef>();
            ::walkExpressionInternal(table_fn.function, depth, zmq_channel);
        }
        break;
    case duckdb::TableReferenceType::JOIN:
        {
            auto& join_ref = table_ref->Cast<duckdb::JoinRef>();
            ::walkTableRef(join_ref.left, depth+1, zmq_channel);
            ::walkTableRef(join_ref.right, depth+1, zmq_channel);
            ::walkExpressionInternal(join_ref.condition, depth+1, zmq_channel);
        }
        break;
    case duckdb::TableReferenceType::SUBQUERY:
        {
            auto& sq_ref = table_ref->Cast<duckdb::SubqueryRef>();
            ::walkSelectStatementInternal(*sq_ref.subquery, 0, zmq_channel);
        }
        break;
    default:
        zmq_channel.warn(std::format("[TODO] Unsupported table ref type: {} (depth: {})", magic_enum::enum_name(table_ref->type), depth));
        break;
    }
}

static auto walkSelectStatementInternal(duckdb::SelectStatement& stmt, uint32_t depth, ZmqChannel& zmq_channel) -> void {
    switch (stmt.node->type) {
    case duckdb::QueryNodeType::SELECT_NODE: 
        {
            auto& select_node =  stmt.node->Cast<duckdb::SelectNode>();
            for (auto& expr: select_node.select_list) {
                ::walkSelectListItem(expr, depth, zmq_channel);
            }
            form_clause: {
                ::walkTableRef(select_node.from_table, depth+1, zmq_channel);
            }
            if (select_node.where_clause) {
                ::walkExpressionInternal(select_node.where_clause, depth+1, zmq_channel);
            }
            if (select_node.groups.group_expressions.size() > 0) {
                zmq_channel.warn(std::format("[TODO] Unsupported group by clause (depth: {})", depth));
            }
            if (select_node.having) {
                zmq_channel.warn(std::format("[TODO] Unsupported having clause (depth: {})", depth));
            }
            if (select_node.sample) {
                zmq_channel.warn(std::format("[TODO] Unsupported sample clause (depth: {})", depth));
            }
        }
        break;
    default: 
        zmq_channel.warn(std::format("[TODO] Unsupported select node: {} (depth: {})", magic_enum::enum_name(stmt.node->type), depth));
        break;
    }
}

auto walkSelectStatement(duckdb::SelectStatement& stmt, ZmqChannel zmq_channel) -> void {
    ::walkSelectStatementInternal(stmt, 0, zmq_channel);
}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

using namespace Catch::Matchers;

auto runTest(const std::string sql, const std::string expected) -> void {
    auto database = duckdb::DuckDB(nullptr);
    auto conn = duckdb::Connection(database);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];

    ::walkSelectStatement(stmt->Cast<duckdb::SelectStatement>(), ZmqChannel::unitTestChannel());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals(expected));
    }
}

TEST_CASE("Convert positional parameter") {
    std::string sql("select $1 as a from Foo");
    std::string expected("SELECT $1 AS a FROM Foo");
    
    runTest(sql, expected);
}

TEST_CASE("Convert positional parameter without alias") {
    std::string sql("select $1 from Foo");
    std::string expected("SELECT $1 FROM Foo");
    
    runTest(sql, expected);
}

TEST_CASE("Convert named parameter") {
    std::string sql("select $value as a from Foo");
    std::string expected("SELECT $1 AS a FROM Foo");
    
    runTest(sql, expected);
}

TEST_CASE("Convert named parameter with type cast") {
    std::string sql("select $value::int as a from Foo");
    std::string expected("SELECT CAST($1 AS INTEGER) AS a FROM Foo");
    
    runTest(sql, expected);
}

TEST_CASE("Convert named parameter without alias") {
    std::string sql("select $v, v from Foo");
    std::string expected(R"(SELECT $1 AS "$v", v FROM Foo)");
    
    runTest(sql, expected);
}

TEST_CASE("Convert named parameter with type cast without alias") {
    std::string sql("select $user_name::text, user_name from Foo");
    std::string expected(R"#(SELECT CAST($1 AS VARCHAR) AS "CAST($user_name AS VARCHAR)", user_name FROM Foo)#");
    
    runTest(sql, expected);
}

TEST_CASE("Convert named parameter as expr without alias") {
    std::string sql("select $x::int + $y::int from Foo");
    std::string expected(R"#(SELECT (CAST($1 AS INTEGER) + CAST($1 AS INTEGER)) AS "(CAST($x AS INTEGER) + CAST($y AS INTEGER))" FROM Foo)#");
    
    runTest(sql, expected);
}

TEST_CASE("Convert named parameter inslude betteen without alias") {
    std::string sql("select $v::int between $x::int and $y::int from Foo");
    std::string expected(R"#(SELECT (CAST($1 AS INTEGER) BETWEEN CAST($1 AS INTEGER) AND CAST($1 AS INTEGER)) AS "(CAST($v AS INTEGER) BETWEEN CAST($x AS INTEGER) AND CAST($y AS INTEGER))" FROM Foo)#");
    
    runTest(sql, expected);
}

TEST_CASE("Convert named parameter inslude case expr without alias#1") {
    std::string sql("select case when $v::int = 0 then $x::int else $y::int end from Foo");
    std::string expected(R"#(SELECT CASE  WHEN ((CAST($1 AS INTEGER) = 0)) THEN (CAST($1 AS INTEGER)) ELSE CAST($1 AS INTEGER) END AS "CASE  WHEN ((CAST($v AS INTEGER) = 0)) THEN (CAST($x AS INTEGER)) ELSE CAST($y AS INTEGER) END" FROM Foo)#");
    
    runTest(sql, expected);
}

TEST_CASE("Convert named parameter inslude case expr without alias#2") {
    std::string sql("select case $v::int when 99 then $x::int else $y::int end from Foo");
    std::string expected(R"#(SELECT CASE  WHEN ((CAST($1 AS INTEGER) = 99)) THEN (CAST($1 AS INTEGER)) ELSE CAST($1 AS INTEGER) END AS "CASE  WHEN ((CAST($v AS INTEGER) = 99)) THEN (CAST($x AS INTEGER)) ELSE CAST($y AS INTEGER) END" FROM Foo)#");
    
    runTest(sql, expected);
}

TEST_CASE("Convert named parameter inslude logical operator without alias") {
    std::string sql("select $x::int = 123 AND $y::text = 'abc' from Foo");
    std::string expected(R"#(SELECT ((CAST($1 AS INTEGER) = 123) AND (CAST($1 AS VARCHAR) = 'abc')) AS "((CAST($x AS INTEGER) = 123) AND (CAST($y AS VARCHAR) = 'abc'))" FROM Foo)#");
    
    runTest(sql, expected);
}

TEST_CASE("Convert named parameter in function args without alias") {
    std::string sql("select string_agg(s, $sep::text) from Foo");
    std::string expected(R"#(SELECT string_agg(s, CAST($1 AS VARCHAR)) AS "string_agg(s, CAST($sep AS VARCHAR))" FROM Foo)#");
    
    runTest(sql, expected);
}

TEST_CASE("Convert named parameter in function args without alias#2") {
    std::string sql("select string_agg(n, $sep::text order by fmod(n, $deg::int) desc) from range(0, 360, 30) t(n)");
    std::string expected(R"#(SELECT string_agg(n, CAST($1 AS VARCHAR) ORDER BY fmod(n, CAST($1 AS INTEGER)) DESC) AS "string_agg(n, CAST($sep AS VARCHAR) ORDER BY fmod(n, CAST($deg AS INTEGER)) DESC)" FROM range(0, 360, 30) AS t(n))#");
    
    runTest(sql, expected);
}

TEST_CASE("Convert named parameter in subquery without alias") {
    std::string sql(R"#(
        select (select Foo.v + Point.x + $offset::int from Point)
        from Foo
    )#");
    std::string expected(R"#(SELECT (SELECT ((Foo.v + Point.x) + CAST($1 AS INTEGER)) FROM Point) AS "(SELECT ((Foo.v + Point.x) + CAST($offset AS INTEGER)) FROM Point)" FROM Foo)#");
    
    runTest(sql, expected);
}

TEST_CASE("Convert named parameter without alias in exists clause") {
    std::string sql(R"#(
        select exists (select Foo.v - Point.x > $diff::int from Point)
        from Foo
    )#");
    std::string expected(R"#(SELECT EXISTS(SELECT ((Foo.v - Point.x) > CAST($1 AS INTEGER)) FROM Point) AS "EXISTS(SELECT ((Foo.v - Point.x) > CAST($diff AS INTEGER)) FROM Point)" FROM Foo)#");
    
    runTest(sql, expected);
}

TEST_CASE("Convert named parameter without alias in any clause") {
    std::string sql(R"#(
        select $v::int = any(select * from range(0, 10, $step::int))
    )#");
    std::string expected(R"#(SELECT (CAST($1 AS INTEGER) = ANY(SELECT * FROM range(0, 10, CAST($1 AS INTEGER)))) AS "(CAST($v AS INTEGER) = ANY(SELECT * FROM range(0, 10, CAST($step AS INTEGER))))")#");
    
    runTest(sql, expected);
}

TEST_CASE("Convert named parameter without alias in any clause#2") {
    std::string sql(R"#(select $v::int = any(range(0, 10, $step::int)))#");
    std::string expected(R"#(SELECT (CAST($1 AS INTEGER) = ANY(SELECT unnest(range(0, 10, CAST($1 AS INTEGER))))) AS "(CAST($v AS INTEGER) = ANY(SELECT unnest(range(0, 10, CAST($step AS INTEGER)))))")#");
    
    runTest(sql, expected);
}

TEST_CASE("Convert named parameter in table function args") {
    std::string sql("select id * 101 from range(0, 10, $step::int) t(id)");
    std::string expected("SELECT (id * 101) FROM range(0, 10, CAST($1 AS INTEGER)) AS t(id)");
    
    runTest(sql, expected);
}

TEST_CASE("Convert named parameter in joined condition") {
    std::string sql("select * from Foo join Bar on Foo.v = Bar.v and Bar.x = $x::int");
    std::string expected("SELECT * FROM Foo INNER JOIN Bar ON (((Foo.v = Bar.v) AND (Bar.x = CAST($1 AS INTEGER))))");
    
    runTest(sql, expected);
}

TEST_CASE("Convert named parameter in derived table (subquery)") {
    std::string sql(R"#(
        select * from (
            select $v::int, $s::text 
        ) v
    )#");
    std::string expected(R"#(SELECT * FROM (SELECT CAST($1 AS INTEGER) AS "CAST($v AS INTEGER)", CAST($1 AS VARCHAR) AS "CAST($s AS VARCHAR)") AS v)#");
    
    runTest(sql, expected);
}

TEST_CASE("Convert named parameter in where clause") {
    std::string sql(R"#(
        select * from Foo
        where v = $v::int and kind = $k::int
    )#");
    std::string expected("SELECT * FROM Foo WHERE ((v = CAST($1 AS INTEGER)) AND (kind = CAST($1 AS INTEGER)))");
    
    runTest(sql, expected);
}

TEST_CASE("Convert named parameter ????") {
    // TODO: not implement
    // order by
    // window function
    //    * filter
    //    * partition
    //    * order by
    //    * frame
}//

#endif
