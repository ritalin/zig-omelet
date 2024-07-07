#include <duckdb.hpp>
#include <duckdb/parser/query_node/list.hpp>
#include <duckdb/parser/tableref/list.hpp>

#define MAGIC_ENUM_RANGE_MAX (std::numeric_limits<uint8_t>::max())

#include <magic_enum/magic_enum.hpp>


#include <iostream>

extern "C" {
    auto ParseDescribeStmt() -> void {
        auto sql = "describe select $a::int, xyz, 123 from Foo";

        auto db = duckdb::DuckDB(nullptr);
        auto conn = duckdb::Connection(db);

        auto stmts = conn.ExtractStatements(sql);
        auto& stmt = stmts[0];

        std::cout << std::format("stmt/type: {}", magic_enum::enum_name(stmt->type)) << std::endl;

        duckdb::SelectStatement& select_stmt = stmt->Cast<duckdb::SelectStatement>();

        std::cout << std::format("node/type: {} ({})", magic_enum::enum_name(select_stmt.node->type), select_stmt.node->type == duckdb::QueryNodeType::SELECT_NODE) << std::endl;
        std::cout << std::format("node/type#2: {}", duckdb::QueryNodeType::SELECT_NODE == duckdb::SelectNode::TYPE) << std::endl;

        auto& root_node = select_stmt.node->Cast<duckdb::SelectNode>();

        std::cout << std::format("select-node/list: {}", root_node.select_list.size()) << std::endl;
        std::cout << std::format("table-ref/type: {}", magic_enum::enum_name(root_node.from_table->type)) << std::endl;

        auto& table_ref = root_node.from_table->Cast<duckdb::ShowRef>();

        std::cout << std::format("showref/type: {} name: {}, node?: {}", 
            magic_enum::enum_name(table_ref.show_type), 
            table_ref.table_name == "" ? "<show query>" : table_ref.table_name, 
            (bool)table_ref.query
        ) << std::endl;

        std::cout << std::format("query: {}", table_ref.ToString()) << std::endl;
    }
}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

using namespace Catch::Matchers;

TEST_CASE("Load schemas") {
    auto db = duckdb::DuckDB(nullptr);
    auto conn = duckdb::Connection(db);

    conn.Query("create table foo(kind int not null, xyz varchar)");

    SECTION("created table information") {
        auto info = conn.TableInfo("Foo");
        REQUIRE(info);
        REQUIRE(info->columns.size() == 2);

        SECTION("find column#1") {
            auto result = std::find_if(info->columns.begin(), info->columns.end(), [](duckdb::ColumnDefinition& c) {
                return c.GetName() == "kind";
            });
            CHECK(result != info->columns.end());

            CHECK_THAT(result->GetName(), Equals("kind"));
            CHECK_THAT(result->GetType().ToString(), Equals("INTEGER"));
        }
        SECTION("find column#2") {
            auto result = std::find_if(info->columns.begin(), info->columns.end(), [](duckdb::ColumnDefinition& c) {
                return c.GetName() == "xyz";
            });
            CHECK(result != info->columns.end());

            CHECK_THAT(result->GetName(), Equals("xyz"));
            CHECK_THAT(result->GetType().ToString(), Equals("VARCHAR"));
        }
    }
}

TEST_CASE("Prepend describe") {
    auto sql = std::string("select $1::int as a, xyz, 123, $2::text as c from Foo");

    auto db = duckdb::DuckDB(nullptr);
    auto conn = duckdb::Connection(db);

    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];

    auto& select_stmt = stmt->Cast<duckdb::SelectStatement>();
    auto& original_node = select_stmt.node;

    auto describe = new duckdb::ShowRef();
    describe->show_type = duckdb::ShowType::DESCRIBE;
    describe->query = std::move(original_node);

    auto describe_node = new duckdb::SelectNode();
    describe_node->from_table.reset(describe);

    select_stmt.node.reset(describe_node);

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals("DESCRIBE (SELECT CAST($1 AS INTEGER) AS a, xyz, 123, CAST($2 AS VARCHAR) AS c FROM Foo)"));
    }
}

TEST_CASE("Describe SELECT list") {
    // auto sql = std::string("DESCRIBE (CAST($1 AS INTEGER) AS a, xyz, SELECT 123, CAST($2 AS VARCHAR) AS c FROM Foo)");
    auto sql = std::string("DESCRIBE (SELECT CAST($2 AS INTEGER) AS a, 123, CAST($1 AS VARCHAR) AS c)");

    auto db = duckdb::DuckDB(nullptr);
    auto conn = duckdb::Connection(db);

    auto stmt = conn.Prepare(sql);
    SECTION("verify prepared statement") {
        REQUIRE_FALSE(stmt->HasError());
    }

    auto param_count = stmt->GetStatementProperties().parameter_count;
    SECTION("verify prepared statement param count") {
        CHECK(param_count == 2);
    }
    auto params = duckdb::vector<duckdb::Value>(param_count);
    auto result = stmt->Execute(params);

    SECTION("verify result") {
        REQUIRE_FALSE(result->HasError());
    }
    SECTION("result columns") {
        CHECK(result->ColumnCount() == 6);
        CHECK_THAT(result->ColumnName(0), Equals("column_name"));
        CHECK_THAT(result->ColumnName(1), Equals("column_type"));
        CHECK_THAT(result->ColumnName(2), Equals("null"));
        CHECK_THAT(result->ColumnName(3), Equals("key"));
        CHECK_THAT(result->ColumnName(4), Equals("default"));
        CHECK_THAT(result->ColumnName(5), Equals("extra"));
    }
    SECTION("result data") {
        auto iter = result->begin();

        SECTION("row data#1") {
            auto row = *iter;

            CHECK(iter != result->end());
            CHECK_THAT(row.GetValue<std::string>(0), Equals("a"));
            CHECK_THAT(row.GetValue<std::string>(1), Equals("INTEGER"));
            CHECK_THAT(row.GetValue<std::string>(2), Equals("YES"));
        }
        ++iter;
        SECTION("row data#2") {
            auto row = *iter;

            CHECK(iter != result->end());
            CHECK_THAT(row.GetValue<std::string>(0), Equals("123"));
            CHECK_THAT(row.GetValue<std::string>(1), Equals("INTEGER"));
            CHECK_THAT(row.GetValue<std::string>(2), Equals("YES"));
        }
        ++iter;
        SECTION("row data#3") {
            auto row = *iter;

            CHECK(iter != result->end());
            CHECK_THAT(row.GetValue<std::string>(0), Equals("c"));
            CHECK_THAT(row.GetValue<std::string>(1), Equals("VARCHAR"));
            CHECK_THAT(row.GetValue<std::string>(2), Equals("YES"));
        }
        ++iter;
        SECTION("validate all iteratation") {
            CHECK_FALSE(iter != result->end());
        }
    }
}

#endif
