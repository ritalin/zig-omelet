#include <duckdb/parser/query_node/list.hpp>
#include <duckdb/parser/tableref/list.hpp>
#include <duckdb/parser/expression/list.hpp>
#include <duckdb/common/extra_type_info.hpp>

#define MAGIC_ENUM_RANGE_MAX (std::numeric_limits<uint8_t>::max())

#include <magic_enum/magic_enum.hpp>

#include "duckdb_params_collector.hpp"
#include "duckdb_binder_support.hpp"

namespace worker {

static auto keepColumnName(duckdb::unique_ptr<duckdb::ParsedExpression>& expr) -> std::string {
    if (expr->alias != "") {
        return expr->alias;
    }
    else {
        return expr->ToString();
    }
}

static auto walkOrderBysNodeInternal(ParameterCollector& collector, duckdb::OrderModifier& order_bys, uint32_t depth) -> void;
static auto walkSelectStatementInternal(ParameterCollector& collector, duckdb::SelectStatement& stmt, uint32_t depth) -> void;

static auto pickUserTypeName(const duckdb::CastExpression& expr) -> std::optional<std::string> {
    auto *ext_info = expr.cast_type.AuxInfo();
    if (ext_info && (ext_info->type == duckdb::ExtraTypeInfoType::USER_TYPE_INFO)) {
        auto& user_info = ext_info->Cast<duckdb::UserTypeInfo>();
        
        return std::make_optional(user_info.user_type_name);
    }

    return std::nullopt;
}

static auto walkExpressionInternal(ParameterCollector& collector, duckdb::unique_ptr<duckdb::ParsedExpression>& expr, uint32_t depth) -> void {
    switch (expr->expression_class) {
    case duckdb::ExpressionClass::PARAMETER:
        {
            auto& param_expr = expr->Cast<duckdb::ParameterExpression>();
            param_expr.identifier = collector.ofPosition(param_expr.identifier);
        }
        break;
    case duckdb::ExpressionClass::CAST: 
        {
            auto& cast_expr = expr->Cast<duckdb::CastExpression>();
            walkExpressionInternal(collector, cast_expr.child, depth+1);
        }
        break;
    case duckdb::ExpressionClass::COMPARISON:
        {
            auto& cmp_expr = expr->Cast<duckdb::ComparisonExpression>();
            walkExpressionInternal(collector, cmp_expr.left, depth+1);
            walkExpressionInternal(collector, cmp_expr.right, depth+1);
        }
        break;
    case duckdb::ExpressionClass::BETWEEN:
        {
            auto& between_expr = expr->Cast<duckdb::BetweenExpression>();
            walkExpressionInternal(collector, between_expr.input, depth+1);
            walkExpressionInternal(collector, between_expr.lower, depth+1);
            walkExpressionInternal(collector, between_expr.upper, depth+1);
        }
        break;
    case duckdb::ExpressionClass::CASE:
        {
            auto& case_expr = expr->Cast<duckdb::CaseExpression>();

            // whrn-then clause
            for (auto& case_check: case_expr.case_checks) {
                walkExpressionInternal(collector, case_check.when_expr, depth+1);
                walkExpressionInternal(collector, case_check.then_expr, depth+1);
            }
            
            // else clause
            walkExpressionInternal(collector, case_expr.else_expr, depth+1);
        }
        break;
    case duckdb::ExpressionClass::CONJUNCTION:
        {
            auto& conj_expr = expr->Cast<duckdb::ConjunctionExpression>();
            
            for (auto& child: conj_expr.children) {
                walkExpressionInternal(collector, child, depth+1);
            }
        }
        break;
    case duckdb::ExpressionClass::FUNCTION:
        {
            auto& fn_expr = expr->Cast<duckdb::FunctionExpression>();

            for (auto& child: fn_expr.children) {
                walkExpressionInternal(collector, child, depth+1);
            }

            // order by(s)
            walkOrderBysNodeInternal(collector, *fn_expr.order_bys, depth+1);
        }
        break;
    case duckdb::ExpressionClass::SUBQUERY:
        {
            auto& sq_expr = expr->Cast<duckdb::SubqueryExpression>();

            // left (if any)
            if (sq_expr.subquery_type == duckdb::SubqueryType::ANY) {
                walkExpressionInternal(collector, sq_expr.child, depth+1);
            }
            // right
            walkSelectStatementInternal(collector, *sq_expr.subquery, depth+1);
        }
        break;
    case duckdb::ExpressionClass::CONSTANT:
    case duckdb::ExpressionClass::COLUMN_REF:
        // no conversion
        break;
    case duckdb::ExpressionClass::OPERATOR:
    default: 
        collector.channel.warn(std::format("[TODO] Unsupported expression class: {} (depth: {})", magic_enum::enum_name(expr->expression_class), depth));
        break;
    }
}

static auto walkOrderBysNodeInternal(ParameterCollector& collector, duckdb::OrderModifier& order_bys, uint32_t depth) -> void {
    for (auto& order_by: order_bys.orders) {
        walkExpressionInternal(collector, order_by.expression, depth);
    }
}

static auto walkSelectListItem(ParameterCollector& collector, duckdb::unique_ptr<duckdb::ParsedExpression>& expr, duckdb::idx_t index, uint32_t depth) -> void {
    if (expr->HasParameter() || expr->HasSubquery()) {
        if (depth > 0) {
            walkExpressionInternal(collector, expr, depth);
        }
        else {
            auto new_alias = keepColumnName(expr);

            walkExpressionInternal(collector, expr, depth);

            if (expr->ToString() != new_alias) {
                expr->alias = new_alias;
            }

            if (expr->expression_class == duckdb::ExpressionClass::CAST) {
                auto user_type_name = pickUserTypeName(expr->Cast<duckdb::CastExpression>());
                if (user_type_name) {

                }
            }
        }
    }
}

static auto walkTableRef(ParameterCollector& collector, duckdb::unique_ptr<duckdb::TableRef>& table_ref, uint32_t depth) -> void {
    switch (table_ref->type) {
    case duckdb::TableReferenceType::BASE_TABLE:
    case duckdb::TableReferenceType::EMPTY_FROM:
        // no conversion
        break;
    case duckdb::TableReferenceType::TABLE_FUNCTION:
        {
            auto& table_fn = table_ref->Cast<duckdb::TableFunctionRef>();
            walkExpressionInternal(collector, table_fn.function, depth);
        }
        break;
    case duckdb::TableReferenceType::JOIN:
        {
            auto& join_ref = table_ref->Cast<duckdb::JoinRef>();
            walkTableRef(collector, join_ref.left, depth+1);
            walkTableRef(collector, join_ref.right, depth+1);

            if (join_ref.condition) { // positional join has NULL pointer
                walkExpressionInternal(collector, join_ref.condition, depth+1);
            }
        }
        break;
    case duckdb::TableReferenceType::SUBQUERY:
        {
            auto& sq_ref = table_ref->Cast<duckdb::SubqueryRef>();
            walkSelectStatementInternal(collector, *sq_ref.subquery, 0);
        }
        break;
    default:
        collector.channel.warn(std::format("[TODO] Unsupported table ref type: {} (depth: {})", magic_enum::enum_name(table_ref->type), depth));
        break;
    }
}

static auto walkStatementResultModifires(ParameterCollector& collector, duckdb::vector<duckdb::unique_ptr<duckdb::ResultModifier>>& modifiers, uint32_t depth) -> void {
    for (auto& modifier: modifiers) {
        switch (modifier->type) {
        case duckdb::ResultModifierType::LIMIT_MODIFIER:
            {
                auto& mod_lim = modifier->Cast<duckdb::LimitModifier>();
                if (mod_lim.offset) {
                    walkExpressionInternal(collector, mod_lim.offset, depth);
                }
                if (mod_lim.limit) {
                    walkExpressionInternal(collector, mod_lim.limit, depth);
                }
            }
            break;
        case duckdb::ResultModifierType::ORDER_MODIFIER:
            walkOrderBysNodeInternal(collector, modifier->Cast<duckdb::OrderModifier>(), depth);
            break;
        case duckdb::ResultModifierType::DISTINCT_MODIFIER: 
            break;
        case duckdb::ResultModifierType::LIMIT_PERCENT_MODIFIER:
            {
                auto& mod_lim = modifier->Cast<duckdb::LimitPercentModifier>();
                if (mod_lim.offset) {
                    walkExpressionInternal(collector, mod_lim.offset, depth);
                }
                if (mod_lim.limit) {
                    walkExpressionInternal(collector, mod_lim.limit, depth);
                }
            }
            break;
        default:
            collector.channel.warn(std::format("[TODO] Not implemented result modifier: {} (depth: {})", magic_enum::enum_name(modifier->type), depth));
            break;
        }
    }
}

static auto walkQueryNode(ParameterCollector& collector, duckdb::unique_ptr<duckdb::QueryNode>& node, uint32_t depth) -> void {
    switch (node->type) {
    case duckdb::QueryNodeType::SELECT_NODE: 
        {
            auto& select_node =  node->Cast<duckdb::SelectNode>();

            cte: {
                collector.walkCTEStatement(node->cte_map);
            }
            
            for (duckdb::idx_t i = 0; auto& expr: select_node.select_list) {
                walkSelectListItem(collector, expr, i, depth);
                ++i;
            }
            form_clause: {
                walkTableRef(collector, select_node.from_table, depth+1);
            }
            if (select_node.where_clause) {
                walkExpressionInternal(collector, select_node.where_clause, depth+1);
            }
            if (select_node.groups.group_expressions.size() > 0) {
                for (auto& expr: select_node.groups.group_expressions) {
                    walkExpressionInternal(collector, expr, depth+1);
                }
            }
            if (select_node.having) {
                walkExpressionInternal(collector, select_node.having, depth+1);
            }
            if (select_node.sample) {
                collector.channel.warn(std::format("[TODO] Unsupported sample clause (depth: {})", depth));
            }
            
            walkStatementResultModifires(collector, select_node.modifiers, depth);
        }
        break;
    case duckdb::QueryNodeType::CTE_NODE: 
        {
            auto& cte_node = node->Cast<duckdb::CTENode>();
            walkQueryNode(collector, cte_node.query, 0);
            walkQueryNode(collector, cte_node.child, 0);
        }
        break;
    default: 
        collector.channel.warn(std::format("[TODO] Unsupported select node: {} (depth: {})", magic_enum::enum_name(node->type), depth));
        break;
    }
}

static auto walkSelectStatementInternal(ParameterCollector& collector, duckdb::SelectStatement& stmt, uint32_t depth) -> void {
    walkQueryNode(collector, stmt.node, depth);
}

auto ParameterCollector::walkSelectStatement(duckdb::SelectStatement& stmt) -> ParameterCollector::Result {
    walkSelectStatementInternal(*this, stmt, 0);

    return {
        .type = StatementType::Select, 
        .names = swapMapEntry(this->name_map), 
    };
}

auto ParameterCollector::walkCTEStatement(duckdb::CommonTableExpressionMap& cte) -> ParameterCollector::Result {
    for (auto& [key, value]: cte.map) {
        walkSelectStatementInternal(*this, *(value->query), 0);
    }

    return {
        .type = StatementType::Select, 
        .names = swapMapEntry(this->name_map), 
    };
}

}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/matchers/catch_matchers_vector.hpp>

using namespace worker;
using namespace Catch::Matchers;

auto runTest(
    const std::string sql, 
    const std::string expected, 
    const ParamNameLookup& lookup) -> void 
{
    auto database = duckdb::DuckDB(nullptr);
    auto conn = duckdb::Connection(database);
    
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];

    auto param_result = 
        ParameterCollector(evalParameterType(stmt), ZmqChannel::unitTestChannel())
        .walkSelectStatement(stmt->Cast<duckdb::SelectStatement>())
    ;

    query: {
        UNSCOPED_INFO("Walk result");
        CHECK_THAT(stmt->ToString(), Equals(expected));
    }
    statement_type: {
        UNSCOPED_INFO("Statement type");
        CHECK(param_result.type == StatementType::Select);
    }
    placeholders: {
        INFO("Placeholder maps");
        map_size: {
            UNSCOPED_INFO("map size");
            REQUIRE(param_result.names.size() == lookup.size());
        }
        param_entries: {
            UNSCOPED_INFO("names entries");

            auto view = param_result.names | std::views::keys;
            auto keys = std::vector<PositionalParam>(view.begin(), view.end());

            for (int i = 1; auto [positional, name]: lookup) {
                param_positional: {
                    UNSCOPED_INFO(std::format("positional key exists#{}", i));
                    CHECK_THAT(keys, VectorContains(positional));
                }
                param_name: {
                    UNSCOPED_INFO(std::format("named value exists#{}", i));
                    CHECK_THAT(name, Equals(lookup.at(positional)));
                }
            }
        }
    }
}

TEST_CASE("Positional parameter") {
    std::string sql("select $1 as a from Foo");
    std::string expected("SELECT $1 AS a FROM Foo");
    ParamNameLookup lookup{ {"1","1"} };

    runTest(sql, expected, lookup);
}

TEST_CASE("Positional parameter without alias") {
    std::string sql("select $1 from Foo where kind = $2");
    std::string expected("SELECT $1 FROM Foo WHERE (kind = $2)");
    ParamNameLookup lookup{ {"1","1"}, {"2","2"} };
    
    runTest(sql, expected, lookup);
}

TEST_CASE("Positional parameter underdering") {
    std::string sql("select $2 as a from Foo where kind = $1");
    std::string expected("SELECT $2 AS a FROM Foo WHERE (kind = $1)");
    ParamNameLookup lookup{ {"2","2"}, {"1","1"} };

    runTest(sql, expected, lookup);
}

TEST_CASE("Auto incremental positional parameter") {
    std::string sql("select $2 as a, ? as b from Foo where kind = $1");
    std::string expected("SELECT $2 AS a, $3 AS b FROM Foo WHERE (kind = $1)");
    ParamNameLookup lookup{ {"2","2"}, {"3","3"}, {"1","1"} };

    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter") {
    std::string sql("select $value as a from Foo where kind = $kind");
    std::string expected("SELECT $1 AS a FROM Foo WHERE (kind = $2)");
    ParamNameLookup lookup{ {"1","value"}, {"2", "kind"} };

    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter with type cast") {
    std::string sql("select $value::int as a from Foo");
    std::string expected("SELECT CAST($1 AS INTEGER) AS a FROM Foo");
    ParamNameLookup lookup{ {"1","value"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter without alias") {
    std::string sql("select $v, v from Foo");
    std::string expected(R"(SELECT $1 AS "$v", v FROM Foo)");
    ParamNameLookup lookup{ {"1","v"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter with type cast without alias") {
    std::string sql("select $user_name::text, user_name from Foo");
    std::string expected(R"#(SELECT CAST($1 AS VARCHAR) AS "CAST($user_name AS VARCHAR)", user_name FROM Foo)#");
    ParamNameLookup lookup{ {"1","user_name"} };

    runTest(sql, expected, lookup);
}

TEST_CASE("Named duplicated parameter with type cast with alias") {
    std::string sql("select $user_name::text u1, $u2_id::int as u2_id, $user_name::text u2, user_name from Foo");
    std::string expected(R"#(SELECT CAST($1 AS VARCHAR) AS u1, CAST($2 AS INTEGER) AS u2_id, CAST($1 AS VARCHAR) AS u2, user_name FROM Foo)#");
    ParamNameLookup lookup{ {"1","user_name"}, {"2","u2_id"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter as expr without alias") {
    std::string sql("select $x::int + $y::int from Foo");
    std::string expected(R"#(SELECT (CAST($1 AS INTEGER) + CAST($2 AS INTEGER)) AS "(CAST($x AS INTEGER) + CAST($y AS INTEGER))" FROM Foo)#");
    ParamNameLookup lookup{ {"1","x"}, {"2","y"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter include betteen without alias") {
    std::string sql("select $v::int between $x::int and $y::int from Foo");
    std::string expected(R"#(SELECT (CAST($1 AS INTEGER) BETWEEN CAST($2 AS INTEGER) AND CAST($3 AS INTEGER)) AS "(CAST($v AS INTEGER) BETWEEN CAST($x AS INTEGER) AND CAST($y AS INTEGER))" FROM Foo)#");
    ParamNameLookup lookup{ {"1","v"}, {"2","x"}, {"3","y"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter include case expr without alias#1") {
    std::string sql("select case when $v::int = 0 then $x::int else $y::int end from Foo");
    std::string expected(R"#(SELECT CASE  WHEN ((CAST($1 AS INTEGER) = 0)) THEN (CAST($2 AS INTEGER)) ELSE CAST($3 AS INTEGER) END AS "CASE  WHEN ((CAST($v AS INTEGER) = 0)) THEN (CAST($x AS INTEGER)) ELSE CAST($y AS INTEGER) END" FROM Foo)#");
    ParamNameLookup lookup{ {"1","v"}, {"2","x"}, {"3","y"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter include case expr without alias#2") {
    std::string sql("select case $v::int when 99 then $y::int else $x::int end from Foo");
    std::string expected(R"#(SELECT CASE  WHEN ((CAST($1 AS INTEGER) = 99)) THEN (CAST($2 AS INTEGER)) ELSE CAST($3 AS INTEGER) END AS "CASE  WHEN ((CAST($v AS INTEGER) = 99)) THEN (CAST($y AS INTEGER)) ELSE CAST($x AS INTEGER) END" FROM Foo)#");
    ParamNameLookup lookup{ {"1","v"}, {"2","y"}, {"3","x"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter include logical operator without alias") {
    std::string sql("select $x::int = 123 AND $y::text = 'abc' from Foo");
    std::string expected(R"#(SELECT ((CAST($1 AS INTEGER) = 123) AND (CAST($2 AS VARCHAR) = 'abc')) AS "((CAST($x AS INTEGER) = 123) AND (CAST($y AS VARCHAR) = 'abc'))" FROM Foo)#");
    ParamNameLookup lookup{ {"1","x"}, {"2","y"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter in function args without alias") {
    std::string sql("select string_agg(s, $sep::text) from Foo");
    std::string expected(R"#(SELECT string_agg(s, CAST($1 AS VARCHAR)) AS "string_agg(s, CAST($sep AS VARCHAR))" FROM Foo)#");
    ParamNameLookup lookup{ {"1","sep"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter in function args without alias#2") {
    std::string sql("select string_agg(n, $sep::text order by fmod(n, $deg::int) desc) from range(0, 360, 30) t(n)");
    std::string expected(R"#(SELECT string_agg(n, CAST($1 AS VARCHAR) ORDER BY fmod(n, CAST($2 AS INTEGER)) DESC) AS "string_agg(n, CAST($sep AS VARCHAR) ORDER BY fmod(n, CAST($deg AS INTEGER)) DESC)" FROM range(0, 360, 30) AS t(n))#");
    ParamNameLookup lookup{ {"1","sep"}, {"2","deg"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter in subquery without alias") {
    std::string sql(R"#(
        select (select Foo.v + Point.x + $offset::int from Point)
        from Foo
    )#");
    std::string expected(R"#(SELECT (SELECT ((Foo.v + Point.x) + CAST($1 AS INTEGER)) FROM Point) AS "(SELECT ((Foo.v + Point.x) + CAST($offset AS INTEGER)) FROM Point)" FROM Foo)#");
    ParamNameLookup lookup{ {"1","offset"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter without alias in exists clause") {
    std::string sql(R"#(
        select exists (select Foo.v - Point.x > $diff::int from Point)
        from Foo
    )#");
    std::string expected(R"#(SELECT EXISTS(SELECT ((Foo.v - Point.x) > CAST($1 AS INTEGER)) FROM Point) AS "EXISTS(SELECT ((Foo.v - Point.x) > CAST($diff AS INTEGER)) FROM Point)" FROM Foo)#");
    ParamNameLookup lookup{ {"1","diff"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter without alias in any clause") {
    std::string sql(R"#(
        select $v::int = any(select * from range(0, 42, $step::int))
    )#");
    std::string expected(R"#(SELECT (CAST($1 AS INTEGER) = ANY(SELECT * FROM range(0, 42, CAST($2 AS INTEGER)))) AS "(CAST($v AS INTEGER) = ANY(SELECT * FROM range(0, 42, CAST($step AS INTEGER))))")#");
    ParamNameLookup lookup{ {"1","v"}, {"2","step"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter without alias in any clause#2") {
    std::string sql(R"#(select $v::int = any(range(0, 10, $step::int)))#");
    std::string expected(R"#(SELECT (CAST($1 AS INTEGER) = ANY(SELECT unnest(range(0, 10, CAST($2 AS INTEGER))))) AS "(CAST($v AS INTEGER) = ANY(SELECT unnest(range(0, 10, CAST($step AS INTEGER)))))")#");
    ParamNameLookup lookup{ {"1","v"}, {"2","step"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter in table function args") {
    std::string sql("select id * 101 from range(0, 10, $step::int) t(id)");
    std::string expected(R"#(SELECT (id * 101) FROM range(0, 10, CAST($1 AS INTEGER)) AS t(id))#");
    ParamNameLookup lookup{ {"1","step"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter in joined condition") {
    std::string sql("select * from Foo join Bar on Foo.v = Bar.v and Bar.x = $x::int");
    std::string expected("SELECT * FROM Foo INNER JOIN Bar ON (((Foo.v = Bar.v) AND (Bar.x = CAST($1 AS INTEGER))))");
    ParamNameLookup lookup{ {"1","x"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter in derived table (subquery)") {
    std::string sql(R"#(
        select * from (
            select $v::int, $s::text 
        ) v
    )#");
    std::string expected(R"#(SELECT * FROM (SELECT CAST($1 AS INTEGER) AS "CAST($v AS INTEGER)", CAST($2 AS VARCHAR) AS "CAST($s AS VARCHAR)") AS v)#");
    ParamNameLookup lookup{ {"1","v"}, {"2","s"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter in where clause") {
    std::string sql(R"#(
        select * from Foo
        where v = $v::int and kind = $k::int
    )#");
    std::string expected("SELECT * FROM Foo WHERE ((v = CAST($1 AS INTEGER)) AND (kind = CAST($2 AS INTEGER)))");
    ParamNameLookup lookup{ {"1","v"}, {"2","k"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter in group by clause") {
    std::string sql(R"#(
        select count( distinct *) as c from Foo
        group by xyz, fmod(id, $weeks::int)
    )#");
    std::string expected("SELECT count(DISTINCT *) AS c FROM Foo GROUP BY xyz, fmod(id, CAST($1 AS INTEGER))");
    ParamNameLookup lookup{ {"1","weeks"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter in having clause") {
    std::string sql(R"#(
        select count( distinct *) as c from Foo
        having fmod(id, $weeks::int) > 0
    )#");
    std::string expected("SELECT count(DISTINCT *) AS c FROM Foo HAVING (fmod(id, CAST($1 AS INTEGER)) > 0)");
    ParamNameLookup lookup{ {"1","weeks"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter in order by clause") {
    std::string sql(R"#(
        select count( distinct *) as c from Foo
        order by fmod(id, $weeks::int)
    )#");
    std::string expected("SELECT count(DISTINCT *) AS c FROM Foo ORDER BY fmod(id, CAST($1 AS INTEGER))");
    ParamNameLookup lookup{ {"1","weeks"} };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter in order by clause (plus limit/offset#1)") {
    std::string sql(R"#(
        select count( distinct *) as c from Foo
        order by fmod(id, $weeks::int)
        offset $off
        limit $min
    )#");
    std::string expected("SELECT count(DISTINCT *) AS c FROM Foo ORDER BY fmod(id, CAST($1 AS INTEGER)) LIMIT $3 OFFSET $2");
    ParamNameLookup lookup{ 
        {"1","weeks"},
        {"2","off"},
        {"3","lim"},
    };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter in order by clause#2 (plus limit/offset#2)") {
    std::string sql(R"#(
        select count( distinct *) as c from Foo
        order by fmod(id, $weeks::int)
        limit $min
        offset $off
    )#");
    std::string expected("SELECT count(DISTINCT *) AS c FROM Foo ORDER BY fmod(id, CAST($1 AS INTEGER)) LIMIT $3 OFFSET $2");
    ParamNameLookup lookup{ 
        {"1","weeks"},
        {"2","off"},
        {"3","lim"},
    };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter in order by clause#3 (plus limit only)") {
    std::string sql(R"#(
        select count( distinct *) as c from Foo
        order by fmod(id, $weeks::int)
        limit $min
    )#");
    std::string expected("SELECT count(DISTINCT *) AS c FROM Foo ORDER BY fmod(id, CAST($1 AS INTEGER)) LIMIT $2");
    ParamNameLookup lookup{ 
        {"1","weeks"},
        {"2","lim"},
    };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter in order by clause#4 (plus offset only)") {
    std::string sql(R"#(
        select count( distinct *) as c from Foo
        order by fmod(id, $weeks::int)
        offset $off
    )#");
    std::string expected("SELECT count(DISTINCT *) AS c FROM Foo ORDER BY fmod(id, CAST($1 AS INTEGER)) OFFSET $2");
    ParamNameLookup lookup{ 
        {"1","weeks"},
        {"2","off"},
    };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter in order by clause#5 (plus percentage limit/offset)") {
    std::string sql(R"#(
        select count( distinct *) as c from Foo
        order by fmod(id, $weeks::int)
        offset $off
        limit $min%
    )#");
    std::string expected("SELECT count(DISTINCT *) AS c FROM Foo ORDER BY fmod(id, CAST($1 AS INTEGER)) LIMIT ($3) % OFFSET $2");
    ParamNameLookup lookup{ 
        {"1","weeks"},
        {"2","off"},
        {"3","lim"},
    };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter in order by clause#6 (plus percent limit only)") {
    std::string sql(R"#(
        select count( distinct *) as c from Foo
        order by fmod(id, $weeks::int)
        limit $min%
    )#");
    std::string expected("SELECT count(DISTINCT *) AS c FROM Foo ORDER BY fmod(id, CAST($1 AS INTEGER)) LIMIT ($2) %");
    ParamNameLookup lookup{ 
        {"1","weeks"},
        {"2","lim"},
    };
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Positional enum parameter (anonymous/select-list)") {
    std::string sql("select $1::ENUM('hide', 'visible') as vis");

    std::string expected("SELECT CAST($1 AS ENUM('hide', 'visible')) AS vis");
    ParamNameLookup lookup{{"1","1"}};
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named enum parameter (anonymous/select-list)") {
    std::string sql("select $vis::ENUM('hide', 'visible') as vis");

    std::string expected("SELECT CAST($1 AS ENUM('hide', 'visible')) AS vis");
    ParamNameLookup lookup{{"1","vis"}};
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Positional enum parameter (predefined/select-list)") {
    std::string sql("select $1::Visibility as vis");

    std::string expected("SELECT CAST($1 AS Visibility) AS vis");
    ParamNameLookup lookup{{"1","1"}};
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named enum parameter (predefined/select-list)") {
    std::string sql("select $vis::Visibility as vis");

    std::string expected("SELECT CAST($1 AS Visibility) AS vis");
    ParamNameLookup lookup{{"1","vis"}};
   
    runTest(sql, expected, lookup);
}

TEST_CASE("CTE (with named parameter)") {
    std::string sql(R"#(
        with ph as (
            select $a::int as a, $b::text as b
        )
        select b, a from ph
    )#");
    std::string expected("WITH ph AS (SELECT CAST($1 AS INTEGER) AS a, CAST($2 AS VARCHAR) AS b)SELECT b, a FROM ph");
    ParamNameLookup lookup{{"1","a"}, {"2", "b"}};
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Not materialized CTE (with named parameter)") {
    std::string sql(R"#(
        with ph as not materialized (
            select $a::int as a, $b::text as b
        )
        select b, a from ph
    )#");
    std::string expected("WITH ph AS (SELECT CAST($1 AS INTEGER) AS a, CAST($2 AS VARCHAR) AS b)SELECT b, a FROM ph");
    ParamNameLookup lookup{{"1","a"}, {"2", "b"}};
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Not materialized CTE#2 (with named parameter)") {
    std::string sql(R"#(
        with ph as not materialized (
            select $a::int as a, $b::text as b, kind from Foo
        )
        select b, a from ph
        where kind = $k
    )#");
    std::string expected("WITH ph AS (SELECT CAST($1 AS INTEGER) AS a, CAST($2 AS VARCHAR) AS b, kind FROM Foo)SELECT b, a FROM ph WHERE (kind = $3)");
    ParamNameLookup lookup{{"1","a"}, {"2", "b"}, {"3", "k"}};
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Default materialized CTE#2 (with named parameter)") {
    std::string sql(R"#(
        with ph as (
            select $a::int as a, $b::text as b, kind from Foo
        )
        select b, a from ph
        where kind = $k
    )#");
    std::string expected("WITH ph AS (SELECT CAST($1 AS INTEGER) AS a, CAST($2 AS VARCHAR) AS b, kind FROM Foo)SELECT b, a FROM ph WHERE (kind = $3)");
    ParamNameLookup lookup{{"1","a"}, {"2", "b"}, {"3", "k"}};
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Materialized CTE#1 (with named parameter)") {
    std::string sql(R"#(
        with ph as materialized (
            select $a::int as a, $b::text as b
        )
        select b, a from ph
    )#");
    std::string expected("WITH ph AS MATERIALIZED (SELECT CAST($1 AS INTEGER) AS a, CAST($2 AS VARCHAR) AS b)SELECT b, a FROM ph");
    ParamNameLookup lookup{{"1","a"}, {"2", "b"}};
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Materialized CTE#2 (with named parameter)") {
    std::string sql(R"#(
        with ph as materialized (
            select $a::int as a, $b::text as b, kind from Foo
        )
        select b, a from ph
        where kind = $k
    )#");
    std::string expected("WITH ph AS MATERIALIZED (SELECT CAST($1 AS INTEGER) AS a, CAST($2 AS VARCHAR) AS b, kind FROM Foo)SELECT b, a FROM ph WHERE (kind = $3)");
    ParamNameLookup lookup{{"1","a"}, {"2", "b"}, {"3", "k"}};
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Materialized CTE#3 (CTEx2)") {
    std::string sql(R"#(
        with
            v as materialized (
                select Foo.id, $a::text as a from Foo
                cross join (
                    select $b::int as b
                )
            ),
            v2 as materialized (
                select $c::text as c
            )
        select id, b, c, a from v
        cross join v2
    )#");
    std::string expected("WITH v AS MATERIALIZED (SELECT Foo.id, CAST($1 AS VARCHAR) AS a FROM Foo , (SELECT CAST($2 AS INTEGER) AS b)), v2 AS MATERIALIZED (SELECT CAST($3 AS VARCHAR) AS c)SELECT id, b, c, a FROM v , v2");

    ParamNameLookup lookup{{"1","a"}, {"2", "b"}, {"3", "c"}};
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Materialized CTE (nested CTE)") {
    std::string sql(R"#(
        with
            v as materialized (
                select Bar.id, $a::text as a from Foo
                cross join (
                    select $b::int as b
                )
            ),
            v2 as materialized (
                select id, b, a from v
            )
        select a, b from v2
    )#");
   
    std::string expected("WITH v AS MATERIALIZED (SELECT Bar.id, CAST($1 AS VARCHAR) AS a FROM Foo , (SELECT CAST($2 AS INTEGER) AS b)), v2 AS MATERIALIZED (SELECT id, b, a FROM v)SELECT a, b FROM v2");

    ParamNameLookup lookup{{"1","a"}, {"2", "b"}};
   
    runTest(sql, expected, lookup);
}

TEST_CASE("Named parameter ????") {
    // TODO: not implement
    // window function
    //    * filter
    //    * partition
    //    * order by
    //    * frame
}//

#endif
