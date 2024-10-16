#include "duckdb_params_collector.hpp"
#include "statement_walker_support.hpp"
#include "duckdb_binder_support.hpp"

namespace worker {

auto ParameterCollector::walkSelectStatement(duckdb::SelectStatement& stmt) -> ParameterCollector::Result {
    walkSelectStatementInternal(*this, stmt, 0);

    return {
        .type = StatementType::Select, 
        .names = swapMapEntry(this->name_map), 
        .examples = std::move(this->examples),
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
    const ParamNameLookup& lookup,
    const ParamExampleLookup& expect_examples) -> void 
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
        INFO("Walk result");
        CHECK_THAT(stmt->ToString(), Equals(expected));
    }
    statement_type: {
        INFO("Statement type");
        CHECK(param_result.type == StatementType::Select);
    }
    placeholders: {
        INFO("Placeholder maps");
        map_size: {
            INFO("map size");
            REQUIRE(param_result.names.size() == lookup.size());
        }
        param_entries: {
            INFO("names entries");

            auto view = param_result.names | std::views::keys;
            auto keys = std::vector<PositionalParam>(view.begin(), view.end());

            for (int i = 1; auto& [positional, entry]: lookup) {
                {
                    param_positional: {
                        INFO(std::format("positional key exists#{}", i));
                        CHECK_THAT(keys, VectorContains(positional));
                    }
                    param_name: {
                        INFO(std::format("named value exists#{}", i));
                        CHECK_THAT(param_result.names.at(positional).name, Equals(entry.name));
                    }
                }
                ++i;
            }
        }
    }
    param_examples: {
        INFO("Param examples");
        examples_size: {
            INFO("Param examples size");
            REQUIRE(param_result.examples.size() == expect_examples.size());
        }

        for (int i = 0; auto& [positional, example]: expect_examples) {
            {
                contains_example: {
                    INFO(std::format("positional key exists#{}", i+1));
                    CHECK(param_result.examples.contains(positional));
                }
                match_example: {
                    INFO(std::format("match example#{}", i+1));
                    CHECK(param_result.examples.at(positional).kind == example.kind);
                    CHECK(param_result.examples.at(positional).value == example.value);
                }
            }
            ++i;
        }
    }
}

TEST_CASE("SelectSQL::Positional parameter") {
    SECTION("With alias") {
        std::string sql("select $1 as a from Foo");
        std::string expected("SELECT $1 AS a FROM Foo");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("1")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("Without alias") {
        std::string sql("select $1 from Foo where kind = $2");
        std::string expected("SELECT $1 FROM Foo WHERE (kind = $2)");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("1")}, {"2", ParamLookupEntry("2")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("underdering") {
        std::string sql("select $2 as a from Foo where kind = $1");
        std::string expected("SELECT $2 AS a FROM Foo WHERE (kind = $1)");
        ParamNameLookup lookup{ {"2", ParamLookupEntry("2")}, {"1", ParamLookupEntry("1")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("Auto increment") {
        std::string sql("select $2 as a, ? as b from Foo where kind = $1");
        std::string expected("SELECT $2 AS a, $3 AS b FROM Foo WHERE (kind = $1)");
        ParamNameLookup lookup{ {"2", ParamLookupEntry("2")}, {"3", ParamLookupEntry("3")}, {"1", ParamLookupEntry("1")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
}

TEST_CASE("SelectSQL::Named parameter") {
    SECTION("basic") {
        std::string sql("select $value as a from Foo where kind = $kind");
        std::string expected("SELECT $1 AS a FROM Foo WHERE (kind = $2)");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("value")}, {"2", ParamLookupEntry("kind")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("With type cast") {
        std::string sql("select $value::int as a from Foo");
        std::string expected("SELECT CAST($1 AS INTEGER) AS a FROM Foo");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("value")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("Without alias") {
        std::string sql("select $v, v from Foo");
        std::string expected(R"(SELECT $1 AS "$v", v FROM Foo)");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("v")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("With type cast, without alias") {
        std::string sql("select $user_name::text, user_name from Foo");
        std::string expected(R"#(SELECT CAST($1 AS VARCHAR) AS "CAST($user_name AS VARCHAR)", user_name FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("user_name")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("Redandant case") {
        std::string sql("select $user_name::text u1, $u2_id::int as u2_id, $user_name::text u2, user_name from Foo");
        std::string expected(R"#(SELECT CAST($1 AS VARCHAR) AS u1, CAST($2 AS INTEGER) AS u2_id, CAST($1 AS VARCHAR) AS u2, user_name FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("user_name")}, {"2", ParamLookupEntry("u2_id")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("Expression without alias") {
        std::string sql("select $x::int + $y::int from Foo");
        std::string expected(R"#(SELECT (CAST($1 AS INTEGER) + CAST($2 AS INTEGER)) AS "(CAST($x AS INTEGER) + CAST($y AS INTEGER))" FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("x")}, {"2", ParamLookupEntry("y")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
}

TEST_CASE("SelectSQL::Named parameter in betteen-expr") {
    SECTION("Without alias") {
        std::string sql("select $v::int between $x::int and $y::int from Foo");
        std::string expected(R"#(SELECT (CAST($1 AS INTEGER) BETWEEN CAST($2 AS INTEGER) AND CAST($3 AS INTEGER)) AS "(CAST($v AS INTEGER) BETWEEN CAST($x AS INTEGER) AND CAST($y AS INTEGER))" FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("v")}, {"2", ParamLookupEntry("x")}, {"3", ParamLookupEntry("y")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
}

TEST_CASE("SelectSQL::Named parameter in case-expr") {
    SECTION("Without alias#1") {
        std::string sql("select case when $v::int = 0 then $x::int else $y::int end from Foo");
        std::string expected(R"#(SELECT CASE  WHEN ((CAST($1 AS INTEGER) = 0)) THEN (CAST($2 AS INTEGER)) ELSE CAST($3 AS INTEGER) END AS "CASE  WHEN ((CAST($v AS INTEGER) = 0)) THEN (CAST($x AS INTEGER)) ELSE CAST($y AS INTEGER) END" FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("v")}, {"2", ParamLookupEntry("x")}, {"3", ParamLookupEntry("y")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("Without alias#2") {
        std::string sql("select case $v::int when 99 then $y::int else $x::int end from Foo");
        std::string expected(R"#(SELECT CASE  WHEN ((CAST($1 AS INTEGER) = 99)) THEN (CAST($2 AS INTEGER)) ELSE CAST($3 AS INTEGER) END AS "CASE  WHEN ((CAST($v AS INTEGER) = 99)) THEN (CAST($y AS INTEGER)) ELSE CAST($x AS INTEGER) END" FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("v")}, {"2", ParamLookupEntry("y")}, {"3", ParamLookupEntry("x")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
}

TEST_CASE("SelectSQL::Named parameter in logical-operator") {
    SECTION("Without alias") {
        std::string sql("select $x::int = 123 AND $y::text = 'abc' from Foo");
        std::string expected(R"#(SELECT ((CAST($1 AS INTEGER) = 123) AND (CAST($2 AS VARCHAR) = 'abc')) AS "((CAST($x AS INTEGER) = 123) AND (CAST($y AS VARCHAR) = 'abc'))" FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("x")}, {"2", ParamLookupEntry("y")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
}

TEST_CASE("SelectSQL::Named parameter in scalar-function") {
    SECTION("Without alias#1") {
        std::string sql("select string_agg(s, $sep::text) from Foo");
        std::string expected(R"#(SELECT string_agg(s, CAST($1 AS VARCHAR)) AS "string_agg(s, CAST($sep AS VARCHAR))" FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("sep")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("Without alias#2") {
        std::string sql("select string_agg(n, $sep::text order by fmod(n, $deg::int) desc) from range(0, 360, 30) t(n)");
        std::string expected(R"#(SELECT string_agg(n, CAST($1 AS VARCHAR) ORDER BY fmod(n, CAST($2 AS INTEGER)) DESC) AS "string_agg(n, CAST($sep AS VARCHAR) ORDER BY fmod(n, CAST($deg AS INTEGER)) DESC)" FROM "range"(0, 360, 30) AS t(n))#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("sep")}, {"2", ParamLookupEntry("deg")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("filter") {
        std::string sql("select id, sum($val::int) filter (fmod(id, $rem::int)) as a from Foo");
        std::string expected(R"#(SELECT id, sum(CAST($1 AS INTEGER)) FILTER (WHERE fmod(id, CAST($2 AS INTEGER))) AS a FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("val")}, {"2", ParamLookupEntry("rem")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
}

TEST_CASE("SelectSQL::Named parameter in window-function") {
    SECTION("Without alias#1") {
        std::string sql("select id, sum($val::int) over () from Foo");
        std::string expected(R"#(SELECT id, sum(CAST($1 AS INTEGER)) OVER () AS "sum(CAST($val AS INTEGER)) OVER ()" FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("val")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("With alias") {
        std::string sql("select id, sum($1::int) over () as a from Foo");
        std::string expected(R"#(SELECT id, sum(CAST($1 AS INTEGER)) OVER () AS a FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("1")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("partition by") {
        std::string sql("select id, sum($val::int) over (partition by fmod(id, $rem::int)) as a from Foo");
        std::string expected(R"#(SELECT id, sum(CAST($1 AS INTEGER)) OVER (PARTITION BY fmod(id, CAST($2 AS INTEGER))) AS a FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("val")}, {"2", ParamLookupEntry("rem")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("order by#1") {
        std::string sql("select id, sum($1::int) over (order by id) as a from Foo");
        std::string expected(R"#(SELECT id, sum(CAST($1 AS INTEGER)) OVER (ORDER BY id) AS a FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("1")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("order by#2") {
        std::string sql("select id, sum($1::int) over (order by id desc) as a from Foo");
        std::string expected(R"#(SELECT id, sum(CAST($1 AS INTEGER)) OVER (ORDER BY id DESC) AS a FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("1")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("filter") {
        std::string sql("select id, sum($val::int) filter (fmod(id, $rem::int)) over () as a from Foo");
        std::string expected(R"#(SELECT id, sum(CAST($1 AS INTEGER)) FILTER (WHERE fmod(id, CAST($2 AS INTEGER))) OVER () AS a FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("val")}, {"2", ParamLookupEntry("rem")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("qualify") {
        std::string sql(R"#(
            select * 
            from Foo
            qualify sum($val::int) over () > 100
        )#");
        std::string expected(R"#(SELECT * FROM Foo QUALIFY (sum(CAST($1 AS INTEGER)) OVER () > 100))#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("val")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("window-clause#1") {
        std::string sql(R"#(
            select id, sum($val::int) over w1 as a 
            from Foo
            window 
                w1 as (
                    partition by fmod(id, $rem::int)
                    order by id desc
                )
        )#");
        std::string expected(R"#(SELECT id, sum(CAST($1 AS INTEGER)) OVER (PARTITION BY fmod(id, CAST($2 AS INTEGER)) ORDER BY id DESC) AS a FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("val")}, {"2", ParamLookupEntry("rem")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("window-clause#2 (reuse)") {
        std::string sql(R"#(
            select id, sum($val::int) over w1 as a, sum(1) over w1 as b
            from Foo
            window 
                w1 as (
                    partition by fmod(id, $rem::int)
                    order by id desc
                )
        )#");
        std::string expected(R"#(SELECT id, sum(CAST($1 AS INTEGER)) OVER (PARTITION BY fmod(id, CAST($2 AS INTEGER)) ORDER BY id DESC) AS a, sum(1) OVER (PARTITION BY fmod(id, CAST($2 AS INTEGER)) ORDER BY id DESC) AS b FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("val")}, {"2", ParamLookupEntry("rem")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("window-clause#3 (clausex2)") {
        std::string sql(R"#(
            select 
                id, 
                sum($val::int) over w1 as a,
                sum(1) over w2 as b
            from range(0, 10, $step::int) t(id)
            window 
                w1 as (
                    partition by fmod(id, $rem::int)
                    order by id desc
                ),
                w2 as (
                    partition by fmod(id, $rem2::int)
                    order by id
                )
        )#");
        std::string expected(R"#(SELECT id, sum(CAST($1 AS INTEGER)) OVER (PARTITION BY fmod(id, CAST($2 AS INTEGER)) ORDER BY id DESC) AS a, sum(1) OVER (PARTITION BY fmod(id, CAST($3 AS INTEGER)) ORDER BY id) AS b FROM "range"(0, 10, CAST($4 AS INTEGER)) AS t(id))#");
        ParamNameLookup lookup{ 
            {"1", ParamLookupEntry("val")}, {"2", ParamLookupEntry("rem")}, 
            {"3", ParamLookupEntry("rem2")}, {"4", ParamLookupEntry("step")} 
        };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
}

TEST_CASE("SelectSQL::Named parameter in builtin window-function") {
    SECTION("row_number") {
        std::string sql("select id, row_number() over (order by id desc) as a from Foo");
        std::string expected(R"#(SELECT id, row_number() OVER (ORDER BY id DESC) AS a FROM Foo)#");
        ParamNameLookup lookup{};
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("ntile") {
        std::string sql("select id, ntile($bucket) over () as a from Foo");
        std::string expected(R"#(SELECT id, ntile($1) OVER () AS a FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("bucket")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("lag") {
        std::string sql("select id, lag(id, $offset, $value_def) over (partition by kind) as a from Foo");
        std::string expected(R"#(SELECT id, lag(id, $1, $2) OVER (PARTITION BY kind) AS a FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("offset")}, {"2", ParamLookupEntry("value_def")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
}

TEST_CASE("SelectSQL::Named parameter in window-function frame") {
    SECTION("raws frame#1 (current row)") {
        std::string sql("select id, sum($val::int) over (rows current row) as a from range(0, 10, $step::int) t(id)");
        std::string expected(R"#(SELECT id, sum(CAST($1 AS INTEGER)) OVER (ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS a FROM "range"(0, 10, CAST($2 AS INTEGER)) AS t(id))#");
        ParamNameLookup lookup{ {"1",ParamLookupEntry("val")}, {"2",ParamLookupEntry("step")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("raws frame#2 (unbounded preceding)") {
        std::string sql("select id, sum($val::int) over (rows unbounded preceding) as a from range(0, 10, $step::int) t(id)");
        std::string expected(R"#(SELECT id, sum(CAST($1 AS INTEGER)) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS a FROM "range"(0, 10, CAST($2 AS INTEGER)) AS t(id))#");
        ParamNameLookup lookup{ {"1",ParamLookupEntry("val")}, {"2",ParamLookupEntry("step")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("raws frame#3 (expr preceding)") {
        std::string sql("select id, sum($val::int) over (rows 5 preceding) as a from range(0, 10, $step::int) t(id)");
        std::string expected(R"#(SELECT id, sum(CAST($1 AS INTEGER)) OVER (ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS a FROM "range"(0, 10, CAST($2 AS INTEGER)) AS t(id))#");
        ParamNameLookup lookup{ {"1",ParamLookupEntry("val")}, {"2",ParamLookupEntry("step")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("rows frame#4 (both unbounded)") {
        std::string sql(R"#(
            select id, 
                sum($val::int) 
                over (
                    rows between unbounded preceding and unbounded following
                ) as a
            from range(0, 10, $step::int) t(id)
        )#");
        std::string expected(R"#(SELECT id, sum(CAST($1 AS INTEGER)) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS a FROM "range"(0, 10, CAST($2 AS INTEGER)) AS t(id))#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("val")}, {"2", ParamLookupEntry("step")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("rows frame#5 (both expr)") {
        std::string sql(R"#(
            select id, 
                sum($val::int) 
                over (
                    rows between $from_row preceding and $to_row following
                ) as a
            from range(0, 10, $step::int) t(id)
        )#");
        std::string expected(R"#(SELECT id, sum(CAST($1 AS INTEGER)) OVER (ROWS BETWEEN $2 PRECEDING AND $3 FOLLOWING) AS a FROM "range"(0, 10, CAST($4 AS INTEGER)) AS t(id))#");
        ParamNameLookup lookup{ 
            {"1", ParamLookupEntry("val")}, {"2", ParamLookupEntry("from_row")}, 
            {"3", ParamLookupEntry("to_row")}, {"4", ParamLookupEntry("step")} 
        };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("range frame#1 (both expr)") {
        std::string sql(R"#(
            select y, month_of_y, record_at, 
                avg(temperature) 
                over (
                    partition by y, month_of_y
                    range between interval ($days) days preceding and current row
                ) as a
            from Temperature
        )#");
        std::string expected(R"#(SELECT y, month_of_y, record_at, avg(temperature) OVER (PARTITION BY y, month_of_y RANGE BETWEEN to_days(CAST(trunc(CAST($1 AS DOUBLE)) AS INTEGER)) PRECEDING AND CURRENT ROW) AS a FROM Temperature)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("days")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
}

TEST_CASE("SelectSQL::Named parameter in table-function") {
    SECTION("function args") {
        std::string sql(R"#(select id * 101 from range("$min_value" := 5, 100, "$step" := 20::int) t(id))#");
        std::string expected(R"#(SELECT (id * 101) FROM "range"($1, 100, CAST($2 AS INTEGER)) AS t(id))#");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("min_value")},
            {"2", ParamLookupEntry("step")} 
        };
        ParamExampleLookup examples{
            {"1", {.kind = ExampleKind::General, .value = duckdb::Value::BIGINT(5)} }, 
            {"2", {.kind = ExampleKind::General, .value = duckdb::Value::INTEGER(20)} },
        };
    
        runTest(sql, expected, lookup, examples);
    }
}

TEST_CASE("SelectSQL::Named parameter in subquery") {
    SECTION("Without alias") {
        std::string sql(R"#(
            select (select Foo.v + Point.x + $offset::int from Point)
            from Foo
        )#");
        std::string expected(R"#(SELECT (SELECT ((Foo.v + Point.x) + CAST($1 AS INTEGER)) FROM Point) AS "(SELECT ((Foo.v + Point.x) + CAST($offset AS INTEGER)) FROM Point)" FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("offset")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("Exists-clause without alias") {
        std::string sql(R"#(
            select exists (select Foo.v - Point.x > $diff::int from Point)
            from Foo
        )#");
        std::string expected(R"#(SELECT EXISTS(SELECT ((Foo.v - Point.x) > CAST($1 AS INTEGER)) FROM Point) AS "EXISTS(SELECT ((Foo.v - Point.x) > CAST($diff AS INTEGER)) FROM Point)" FROM Foo)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("diff")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("Any-clause without alias#1") {
        std::string sql(R"#(
            select $v::int = any(select * from range(0, 42, $step::int))
        )#");
        std::string expected(R"#(SELECT (CAST($1 AS INTEGER) = ANY(SELECT * FROM "range"(0, 42, CAST($2 AS INTEGER)))) AS "(CAST($v AS INTEGER) = ANY(SELECT * FROM ""range""(0, 42, CAST($step AS INTEGER))))")#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("v")}, {"2", ParamLookupEntry("step")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("Any-clause without alias#2") {
        std::string sql(R"#(select $v::int = any(range(0, 10, $step::int)))#");
        std::string expected(R"#(SELECT (CAST($1 AS INTEGER) = ANY(SELECT unnest("range"(0, 10, CAST($2 AS INTEGER))))) AS "(CAST($v AS INTEGER) = ANY(SELECT unnest(""range""(0, 10, CAST($step AS INTEGER)))))")#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("v")}, {"2", ParamLookupEntry("step")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("Derived table") {
        std::string sql(R"#(
            select * from (
                select $v::int, $s::text 
            ) v
        )#");
        std::string expected(R"#(SELECT * FROM (SELECT CAST($1 AS INTEGER) AS "CAST($v AS INTEGER)", CAST($2 AS VARCHAR) AS "CAST($s AS VARCHAR)") AS v)#");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("v")}, {"2", ParamLookupEntry("s")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
}

TEST_CASE("SelectSQL::Named parameter in joined condition") {
    std::string sql("select * from Foo join Bar on Foo.v = Bar.v and Bar.x = $x::int");
    std::string expected("SELECT * FROM Foo INNER JOIN Bar ON (((Foo.v = Bar.v) AND (Bar.x = CAST($1 AS INTEGER))))");
    ParamNameLookup lookup{ {"1", ParamLookupEntry("x")} };
    ParamExampleLookup examples{};
    
    runTest(sql, expected, lookup, examples);
}

TEST_CASE("SelectSQL::Named parameter in clauses") {
    SECTION("Where-clause") {
        std::string sql(R"#(
            select * from Foo
            where v = $v::int and kind = $k::int
        )#");
        std::string expected("SELECT * FROM Foo WHERE ((v = CAST($1 AS INTEGER)) AND (kind = CAST($2 AS INTEGER)))");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("v")}, {"2", ParamLookupEntry("k")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("Group-clause") {
        std::string sql(R"#(
            select count( distinct *) as c from Foo
            group by xyz, fmod(id, $weeks::int)
        )#");
        std::string expected("SELECT count(DISTINCT *) AS c FROM Foo GROUP BY xyz, fmod(id, CAST($1 AS INTEGER))");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("weeks")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("Having-clause") {
        std::string sql(R"#(
            select count( distinct *) as c from Foo
            having fmod(id, $weeks::int) > 0
        )#");
        std::string expected("SELECT count(DISTINCT *) AS c FROM Foo HAVING (fmod(id, CAST($1 AS INTEGER)) > 0)");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("weeks")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
}

TEST_CASE("SelectSQL::Named parameter in order-clause") {
    SECTION("basic") {
        std::string sql(R"#(
            select count( distinct *) as c from Foo
            order by fmod(id, $weeks::int)
        )#");
        std::string expected("SELECT count(DISTINCT *) AS c FROM Foo ORDER BY fmod(id, CAST($1 AS INTEGER))");
        ParamNameLookup lookup{ {"1", ParamLookupEntry("weeks")} };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("With limit/offset#1") {
        std::string sql(R"#(
            select count( distinct *) as c from Foo
            order by fmod(id, $weeks::int)
            offset $off
            limit $lim
        )#");
        std::string expected("SELECT count(DISTINCT *) AS c FROM Foo ORDER BY fmod(id, CAST($1 AS INTEGER)) LIMIT $3 OFFSET $2");
        ParamNameLookup lookup{ 
            {"1", ParamLookupEntry("weeks")},
            {"2", ParamLookupEntry("off")},
            {"3", ParamLookupEntry("lim")},
        };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("With limit/offset#2") {
        std::string sql(R"#(
            select count( distinct *) as c from Foo
            order by fmod(id, $weeks::int)
            limit $lim
            offset $off
        )#");
        std::string expected("SELECT count(DISTINCT *) AS c FROM Foo ORDER BY fmod(id, CAST($1 AS INTEGER)) LIMIT $3 OFFSET $2");
        ParamNameLookup lookup{ 
            {"1", ParamLookupEntry("weeks")},
            {"2", ParamLookupEntry("off")},
            {"3", ParamLookupEntry("lim")},
        };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("With limit only") {
        std::string sql(R"#(
            select count( distinct *) as c from Foo
            order by fmod(id, $weeks::int)
            limit $lim
        )#");
        std::string expected("SELECT count(DISTINCT *) AS c FROM Foo ORDER BY fmod(id, CAST($1 AS INTEGER)) LIMIT $2");
        ParamNameLookup lookup{ 
            {"1", ParamLookupEntry("weeks")},
            {"2", ParamLookupEntry("lim")},
        };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("With offset only)") {
        std::string sql(R"#(
            select count( distinct *) as c from Foo
            order by fmod(id, $weeks::int)
            offset $off
        )#");
        std::string expected("SELECT count(DISTINCT *) AS c FROM Foo ORDER BY fmod(id, CAST($1 AS INTEGER)) OFFSET $2");
        ParamNameLookup lookup{ 
            {"1", ParamLookupEntry("weeks")},
            {"2", ParamLookupEntry("off")},
        };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("With percentage limit/offset") {
        std::string sql(R"#(
            select count( distinct *) as c from Foo
            order by fmod(id, $weeks::int)
            offset $off
            limit $lim%
        )#");
        std::string expected("SELECT count(DISTINCT *) AS c FROM Foo ORDER BY fmod(id, CAST($1 AS INTEGER)) LIMIT ($3) % OFFSET $2");
        ParamNameLookup lookup{ 
            {"1", ParamLookupEntry("weeks")},
            {"2", ParamLookupEntry("off")},
            {"3", ParamLookupEntry("lim")},
        };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("With percent limit only") {
        std::string sql(R"#(
            select count( distinct *) as c from Foo
            order by fmod(id, $weeks::int)
            limit $lim%
        )#");
        std::string expected("SELECT count(DISTINCT *) AS c FROM Foo ORDER BY fmod(id, CAST($1 AS INTEGER)) LIMIT ($2) %");
        ParamNameLookup lookup{ 
            {"1", ParamLookupEntry("weeks")},
            {"2", ParamLookupEntry("lim")},
        };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
}

TEST_CASE("SelectSQL::ENUM parameter/ENUM") {
    SECTION("positional") {
        SECTION("anonymous/select-list") {
            std::string sql("select $1::ENUM('hide', 'visible') as vis");

            std::string expected("SELECT CAST($1 AS ENUM('hide', 'visible')) AS vis");
            ParamNameLookup lookup{{"1", ParamLookupEntry("1")}};
            ParamExampleLookup examples{};
            
            runTest(sql, expected, lookup, examples);
        }
        SECTION("predefined/select-list") {
            std::string sql("select $1::Visibility as vis");

            std::string expected("SELECT CAST($1 AS Visibility) AS vis");
            ParamNameLookup lookup{{"1", ParamLookupEntry("1")}};
            ParamExampleLookup examples{};
            
            runTest(sql, expected, lookup, examples);
        }
    }
    SECTION("Named") {
        SECTION("anonymous/select-list") {
            std::string sql("select $vis::ENUM('hide', 'visible') as vis");

            std::string expected("SELECT CAST($1 AS ENUM('hide', 'visible')) AS vis");
            ParamNameLookup lookup{{"1", ParamLookupEntry("vis")}};
            ParamExampleLookup examples{};
            
            runTest(sql, expected, lookup, examples);
        }
        SECTION("predefined/select-list") {
            std::string sql("select $vis::Visibility as vis");

            std::string expected("SELECT CAST($1 AS Visibility) AS vis");
            ParamNameLookup lookup{{"1", ParamLookupEntry("vis")}};
            ParamExampleLookup examples{};
            
            runTest(sql, expected, lookup, examples);
        }
    }
}

TEST_CASE("SelectSQL::values clause") {
    SECTION("default#1") {
        std::string sql("values ($id_1::int, $name_1::text, $age_1::int), ($id_2::int, $name_2::text, $age_2::int), ");

        std::string expected("SELECT * FROM (VALUES (CAST($1 AS INTEGER), CAST($2 AS VARCHAR), CAST($3 AS INTEGER)), (CAST($4 AS INTEGER), CAST($5 AS VARCHAR), CAST($6 AS INTEGER))) AS valueslist");
            ParamNameLookup lookup{
                {"1", ParamLookupEntry("id_1")}, {"2", ParamLookupEntry("name_1")}, {"3", ParamLookupEntry("age_1")}, 
                {"4", ParamLookupEntry("id_2")}, {"5", ParamLookupEntry("name_2")}, {"6", ParamLookupEntry("age_2")}, 
            };
            ParamExampleLookup examples{};
            
            runTest(sql, expected, lookup, examples);
    }
}

TEST_CASE("SelectSQL::CTE") {
    SECTION("default#1") {
        std::string sql(R"#(
            with ph as (
                select $a::int as a, $b::text as b
            )
            select b, a from ph
        )#");
        std::string expected("WITH ph AS (SELECT CAST($1 AS INTEGER) AS a, CAST($2 AS VARCHAR) AS b)SELECT b, a FROM ph");
        ParamNameLookup lookup{{"1", ParamLookupEntry("a")}, {"2", ParamLookupEntry("b")}};
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("default#2 (plus where-clause parameter)") {
        std::string sql(R"#(
            with ph as (
                select $a::int as a, $b::text as b, kind from Foo
            )
            select b, a from ph
            where kind = $k
        )#");
        std::string expected("WITH ph AS (SELECT CAST($1 AS INTEGER) AS a, CAST($2 AS VARCHAR) AS b, kind FROM Foo)SELECT b, a FROM ph WHERE (kind = $3)");
        ParamNameLookup lookup{{"1", ParamLookupEntry("a")}, {"2", ParamLookupEntry("b")}, {"3", ParamLookupEntry("k")}};
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("Not materialized#1") {
        std::string sql(R"#(
            with ph as not materialized (
                select $a::int as a, $b::text as b
            )
            select b, a from ph
        )#");
        std::string expected("WITH ph AS NOT MATERIALIZED (SELECT CAST($1 AS INTEGER) AS a, CAST($2 AS VARCHAR) AS b)SELECT b, a FROM ph");
        ParamNameLookup lookup{{"1", ParamLookupEntry("a")}, {"2", ParamLookupEntry("b")}};
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("Not materialized CTE#2 (plus where-clause parameter)") {
        std::string sql(R"#(
            with ph as not materialized (
                select $a::int as a, $b::text as b, kind from Foo
            )
            select b, a from ph
            where kind = $k
        )#");
        std::string expected("WITH ph AS NOT MATERIALIZED (SELECT CAST($1 AS INTEGER) AS a, CAST($2 AS VARCHAR) AS b, kind FROM Foo)SELECT b, a FROM ph WHERE (kind = $3)");
        ParamNameLookup lookup{{"1", ParamLookupEntry("a")}, {"2", ParamLookupEntry("b")}, {"3", ParamLookupEntry("k")}};
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
}

TEST_CASE("SelectSQL::Materialized CTE") {
    SECTION("basic#1") {
        std::string sql(R"#(
            with ph as materialized (
                select $a::int as a, $b::text as b
            )
            select b, a from ph
        )#");
        std::string expected("WITH ph AS MATERIALIZED (SELECT CAST($1 AS INTEGER) AS a, CAST($2 AS VARCHAR) AS b)SELECT b, a FROM ph");
        ParamNameLookup lookup{{"1", ParamLookupEntry("a")}, {"2", ParamLookupEntry("b")}};
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("basic#2") {
        std::string sql(R"#(
            with ph as materialized (
                select $a::int as a, $b::text as b, kind from Foo
            )
            select b, a from ph
            where kind = $k
        )#");
        std::string expected("WITH ph AS MATERIALIZED (SELECT CAST($1 AS INTEGER) AS a, CAST($2 AS VARCHAR) AS b, kind FROM Foo)SELECT b, a FROM ph WHERE (kind = $3)");
        ParamNameLookup lookup{{"1", ParamLookupEntry("a")}, {"2", ParamLookupEntry("b")}, {"3", ParamLookupEntry("k")}};
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("CTEx2") {
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

        ParamNameLookup lookup{{"1", ParamLookupEntry("a")}, {"2", ParamLookupEntry("b")}, {"3", ParamLookupEntry("c")}};
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("nested") {
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

        ParamNameLookup lookup{{"1", ParamLookupEntry("a")}, {"2", ParamLookupEntry("b")}};
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
}

TEST_CASE("SelectSQL::Recursive CTE") {
    SECTION("default CTE") {
        std::string sql(R"#(
            with recursive t(n) AS (
                VALUES ($min_value::int)
                UNION ALL
                SELECT n+1 FROM t WHERE n < $max_value::int
            )
            SELECT n FROM t
        )#");
        std::string expected("WITH RECURSIVE t (n) AS ((SELECT * FROM (VALUES (CAST($1 AS INTEGER))) AS valueslist) UNION  ALL (SELECT (n + 1) FROM t WHERE (n < CAST($2 AS INTEGER))))SELECT n FROM t");

        ParamNameLookup lookup{{"1", ParamLookupEntry("min_value")}, {"2", ParamLookupEntry("max_value")}};
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("default CTEx2") {
        std::string sql(R"#(
            with recursive 
                t(n) AS (
                    VALUES ($min_value::int)
                    UNION ALL
                    SELECT n+1 FROM t WHERE n < $max_value::int
                ),
                t2(m) AS (
                    VALUES ($min_value::int)
                    UNION ALL
                    SELECT m*2 FROM t2 WHERE m < $max_value2::int
                )
            SELECT n, m FROM t positional join t2
        )#");
        std::string expected("WITH RECURSIVE t (n) AS ((SELECT * FROM (VALUES (CAST($1 AS INTEGER))) AS valueslist) UNION  ALL (SELECT (n + 1) FROM t WHERE (n < CAST($2 AS INTEGER)))), t2 (m) AS ((SELECT * FROM (VALUES (CAST($1 AS INTEGER))) AS valueslist) UNION  ALL (SELECT (m * 2) FROM t2 WHERE (m < CAST($3 AS INTEGER))))SELECT n, m FROM t POSITIONAL JOIN t2");

        ParamNameLookup lookup{{"1", ParamLookupEntry("min_value")}, {"2", ParamLookupEntry("max_value")}, {"3", ParamLookupEntry("max_value2")}};
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("default CTE (nested)") {
        std::string sql(R"#(
            with recursive
                t(n) AS (
                    VALUES ($min_value::int)
                    UNION ALL
                    SELECT n+1 FROM t WHERE n < $max_value::int
                ),
                t2(m) as (
                    select n + $delta::int as m from t
                    union all
                    select m*2 from t2 where m < $max_value2::int
                )
            SELECT m FROM t2
        )#");
        std::string expected("WITH RECURSIVE t (n) AS ((SELECT * FROM (VALUES (CAST($1 AS INTEGER))) AS valueslist) UNION  ALL (SELECT (n + 1) FROM t WHERE (n < CAST($2 AS INTEGER)))), t2 (m) AS ((SELECT (n + CAST($3 AS INTEGER)) AS m FROM t) UNION  ALL (SELECT (m * 2) FROM t2 WHERE (m < CAST($4 AS INTEGER))))SELECT m FROM t2");

        ParamNameLookup lookup{
            {"1", ParamLookupEntry("min_value")}, {"2", ParamLookupEntry("max_value")},
            {"3", ParamLookupEntry("delta")}, {"4", ParamLookupEntry("max_value2")}
        };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("Not matirialized CTE") {
        std::string sql(R"#(
            with recursive t(n) AS not materialized (
                VALUES ($min_value::int)
                UNION ALL
                SELECT n+1 FROM t WHERE n < $max_value::int
            )
            SELECT n FROM t
        )#");
        std::string expected("WITH RECURSIVE t (n) AS NOT MATERIALIZED ((SELECT * FROM (VALUES (CAST($1 AS INTEGER))) AS valueslist) UNION  ALL (SELECT (n + 1) FROM t WHERE (n < CAST($2 AS INTEGER))))SELECT n FROM t");

        ParamNameLookup lookup{{"1", ParamLookupEntry("min_value")}, {"2", ParamLookupEntry("max_value")}};
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
}

TEST_CASE("SelectSQL::Recursive matitealized CTE") {
    SECTION("Matirialized CTE") {
        std::string sql(R"#(
            with recursive t(n) AS materialized (
                VALUES ($min_value::int)
                UNION ALL
                SELECT n+1 FROM t WHERE n < $max_value::int
            )
            SELECT n FROM t
        )#");
        std::string expected("WITH RECURSIVE t (n) AS MATERIALIZED ((SELECT * FROM (VALUES (CAST($1 AS INTEGER))) AS valueslist) UNION  ALL (SELECT (n + 1) FROM t WHERE (n < CAST($2 AS INTEGER))))SELECT n FROM t");

        ParamNameLookup lookup{{"1", ParamLookupEntry("min_value")}, {"2", ParamLookupEntry("max_value")}};
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("Matirialized CTEx2") {
        std::string sql(R"#(
            with recursive 
                t(n) AS materialized (
                    VALUES ($min_value::int)
                    UNION ALL
                    SELECT n+1 FROM t WHERE n < $max_value::int
                ),
                t2(m) AS materialized (
                    VALUES ($min_value::int)
                    UNION ALL
                    SELECT m*2 FROM t2 WHERE m < $max_value2::int
                )
            SELECT n, m FROM t positional join t2
        )#");
        std::string expected("WITH RECURSIVE t (n) AS MATERIALIZED ((SELECT * FROM (VALUES (CAST($1 AS INTEGER))) AS valueslist) UNION  ALL (SELECT (n + 1) FROM t WHERE (n < CAST($2 AS INTEGER)))), t2 (m) AS MATERIALIZED ((SELECT * FROM (VALUES (CAST($1 AS INTEGER))) AS valueslist) UNION  ALL (SELECT (m * 2) FROM t2 WHERE (m < CAST($3 AS INTEGER))))SELECT n, m FROM t POSITIONAL JOIN t2");

        ParamNameLookup lookup{{"1", ParamLookupEntry("min_value")}, {"2", ParamLookupEntry("max_value")}, {"3", ParamLookupEntry("max_value2")}};
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
    SECTION("materialized CTE (nested)") {
        std::string sql(R"#(
            with recursive
                t(n) AS materialized (
                    VALUES ($min_value::int)
                    UNION ALL
                    SELECT n+1 FROM t WHERE n < $max_value::int
                ),
                t2(m) as materialized (
                    select n + $delta::int as m from t
                    union all
                    select m*2 from t2 where m < $max_value2::int
                )
            SELECT m FROM t2
        )#");
        std::string expected("WITH RECURSIVE t (n) AS MATERIALIZED ((SELECT * FROM (VALUES (CAST($1 AS INTEGER))) AS valueslist) UNION  ALL (SELECT (n + 1) FROM t WHERE (n < CAST($2 AS INTEGER)))), t2 (m) AS MATERIALIZED ((SELECT (n + CAST($3 AS INTEGER)) AS m FROM t) UNION  ALL (SELECT (m * 2) FROM t2 WHERE (m < CAST($4 AS INTEGER))))SELECT m FROM t2");

        ParamNameLookup lookup{
            {"1", ParamLookupEntry("min_value")}, {"2", ParamLookupEntry("max_value")},
            {"3", ParamLookupEntry("delta")}, {"4", ParamLookupEntry("max_value2")}
        };
        ParamExampleLookup examples{};
        
        runTest(sql, expected, lookup, examples);
    }
}

TEST_CASE("SelectSQL::Set-operator") {
    std::string sql(R"#(
        select id from Foo where id > $n1
        union all
        select id from Bar where id <= $n2
    )#");

    std::string expected("(SELECT id FROM Foo WHERE (id > $1)) UNION ALL (SELECT id FROM Bar WHERE (id <= $2))");

    ParamNameLookup lookup{{"1", ParamLookupEntry("n1")}, {"2", ParamLookupEntry("n2")}};
    ParamExampleLookup examples{};
    
    runTest(sql, expected, lookup, examples);
}

#endif
