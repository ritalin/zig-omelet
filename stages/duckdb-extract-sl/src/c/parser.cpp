#include <duckdb.hpp>
#include <duckdb/parser/query_node/list.hpp>
#include <duckdb/parser/tableref/list.hpp>
#include <duckdb/parser/expression/list.hpp>

#define MAGIC_ENUM_RANGE_MAX (std::numeric_limits<uint8_t>::max())

#include <magic_enum/magic_enum.hpp>

#include "duckdb_database.hpp"
#include "duckdb_worker.h"

#include <iostream>

class SelectListCollector {
public:
    SelectListCollector(worker::Database *db, std::string id, std::optional<void *> socket) 
        : conn(std::move(db->connect())), id(id), socket(std::move(socket)) {}
public:
    auto execute(std::string query) -> WorkerResultCode;
public:
    auto warn(const std::string& message) -> void;
    auto error(const std::string& message) -> void;
private:
    duckdb::Connection conn;
    std::string id;
    std::optional<void *> socket;
private:
    friend auto borrowConnection(SelectListCollector& collector) -> duckdb::Connection&;
};

auto borrowConnection(SelectListCollector& collector) -> duckdb::Connection& {
    return collector.conn;
}

static auto prependDescribeKeyword(duckdb::SelectStatement& stmt) -> void {
    auto& original_node = stmt.node;

    auto describe = new duckdb::ShowRef();
    describe->show_type = duckdb::ShowType::DESCRIBE;
    describe->query = std::move(original_node);

    auto describe_node = new duckdb::SelectNode();
    describe_node->from_table.reset(describe);

    stmt.node.reset(describe_node);
}

static auto walkExpression(SelectListCollector& collector, duckdb::unique_ptr<duckdb::ParsedExpression>& expr) -> void {
    if (expr->HasParameter() || expr->HasSubquery()) {
        switch (expr->expression_class) {
        case duckdb::ExpressionClass::PARAMETER:
            {
                auto& param_expr = expr->Cast<duckdb::ParameterExpression>();
                param_expr.identifier = "1";
            }
            break;
        case duckdb::ExpressionClass::CAST: 

        case duckdb::ExpressionClass::FUNCTION:

        case duckdb::ExpressionClass::COMPARISON:

        case duckdb::ExpressionClass::BETWEEN:

        case duckdb::ExpressionClass::CASE:

        case duckdb::ExpressionClass::SUBQUERY:
        default: 
            collector.warn(std::format("[TODO] Unsupported expression class: {}", magic_enum::enum_name(expr->expression_class)));
            break;
        }
    }
}

static auto convertToPositionalParameter(SelectListCollector& collector, duckdb::SelectStatement& stmt) -> void {
    switch (stmt.node->type) {
    case duckdb::QueryNodeType::SELECT_NODE: 
        {
            auto& select_node =  stmt.node->Cast<duckdb::SelectNode>();
            for (auto& expr: select_node.select_list) {
                ::walkExpression(collector, expr);
            }
        }
        break;
    default: 
        collector.warn(std::format("[TODO] Unsupported select node: {}", magic_enum::enum_name(stmt.node->type)));
        break;
    }
}

static auto sendLog(void *socket, const std::string& log_level, const std::string& message) -> void {

}

static auto sendWorkerResult(const std::optional<void *>& socket) -> void {

}

auto SelectListCollector::execute(std::string query) -> WorkerResultCode {
    std::string message;
    try {
        auto stmts = this->conn.ExtractStatements(query);

        for (auto& stmt: stmts) {
            if (stmt->type == duckdb::StatementType::SELECT_STATEMENT) {
                auto& select_stmt = stmt->Cast<duckdb::SelectStatement>();
                ::convertToPositionalParameter(*this, select_stmt);
                ::prependDescribeKeyword(select_stmt);
            }
            else {
                // send empty result
                ::sendWorkerResult(this->socket);
            }
        }

        return no_error;
    }
    catch (const duckdb::ParserException& ex) {
        message = ex.what();
    }
    
    this->error(message);
    return invalid_sql;
}

auto SelectListCollector::warn(const std::string& message) -> void {
    if (this->socket) {
        ::sendLog(this->socket.value(), "warn", message);
    }
    else {
        std::cout << std::format("warn: {}", message) << std::endl;
    }
}
auto SelectListCollector::error(const std::string& message) -> void {
    if (this->socket) {
        ::sendLog(this->socket.value(), "err", message);
    }
    else {
        std::cout << std::format("err: {}", message) << std::endl;
    }
}


extern "C" {
    auto initCollector(DatabaseRef db_ref, const char *id, size_t id_len, void *socket, CollectorRef *handle) -> int32_t {
        auto db = reinterpret_cast<worker::Database *>(db_ref);
        auto collector = new SelectListCollector(db, std::string(id, id_len), socket);

        *handle = reinterpret_cast<CollectorRef>(collector);
        return 0;
    }

    auto deinitCollector(CollectorRef handle) -> void {
        delete reinterpret_cast<SelectListCollector *>(handle);
    }

    auto executeDescribe(CollectorRef handle, const char *query, size_t query_len) -> void {
    }

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

TEST_CASE("Error SQL") {
    auto sql = std::string("SELT $1::int as a");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);
    
    auto err = collector.execute(sql);

    SECTION("execute result code") {
        CHECK(err != 0);
    }
}

TEST_CASE("Convert named parameter to positional parameter") {
    auto sql = std::string("select $value as a from Foo");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::convertToPositionalParameter(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals("SELECT $1 AS a FROM Foo"));
    }
}

TEST_CASE("Prepend describe") {
    auto sql = std::string("select $1::int as a, xyz, 123, $2::text as c from Foo");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto conn = db.connect();;
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::prependDescribeKeyword(stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals("DESCRIBE (SELECT CAST($1 AS INTEGER) AS a, xyz, 123, CAST($2 AS VARCHAR) AS c FROM Foo)"));
    }
}

TEST_CASE("Not exist relation") {
    auto sql = std::string("SELT $1::int as p from Origin");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);


}

TEST_CASE("Describe SELECT list") {
    auto sql = std::string("DESCRIBE (SELECT CAST($1 AS INTEGER) AS a, 123, CAST($1 AS VARCHAR) AS c)");

    auto db = duckdb::DuckDB(nullptr);
    auto conn = duckdb::Connection(db);

    auto stmt = conn.Prepare(sql);
    SECTION("verify prepared statement") {
        REQUIRE_FALSE(stmt->HasError());
    }

    auto param_count = stmt->GetStatementProperties().parameter_count;
    SECTION("verify prepared statement param count") {
        CHECK(param_count == 1);
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
