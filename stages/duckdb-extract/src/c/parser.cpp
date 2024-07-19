#include <duckdb.hpp>
#include <duckdb/parser/query_node/list.hpp>
#include <duckdb/parser/tableref/list.hpp>
#include <duckdb/parser/expression/list.hpp>

#define MAGIC_ENUM_RANGE_MAX (std::numeric_limits<uint8_t>::max())

#include <magic_enum/magic_enum.hpp>
#include <zmq.h>

#include "duckdb_worker.h"
#include "duckdb_database.hpp"
#include "zmq_worker_support.hpp"
#include "duckdb_binder_support.hpp"

#include <iostream>

#include "cbor_encode.hpp"

namespace worker {

class SelectListCollector {
public:
    duckdb::Connection conn;
public:
    SelectListCollector(worker::Database *db, std::string id, std::optional<void *> socket) 
        : conn(std::move(db->connect())), id(id), socket(socket) {}
public:
    auto execute(std::string query) -> WorkerResultCode;
public:
    auto messageChannel(const std::string& from) -> ZmqChannel;
private:
    std::string id;
    std::optional<void *> socket;
};

struct DescribeResult {
    std::string field_name;
    std::string field_type;
    bool nullable;
};

static auto prependDescribeKeyword(duckdb::SelectStatement& stmt) -> void {
    auto& original_node = stmt.node;

    auto describe = new duckdb::ShowRef();
    describe->show_type = duckdb::ShowType::DESCRIBE;
    describe->query = std::move(original_node);

    auto describe_node = new duckdb::SelectNode();
    describe_node->from_table.reset(describe);
    describe_node->select_list.push_back(duckdb::StarExpression().Copy());

    stmt.node.reset(describe_node);

    stmt.named_param_map = {{"1", 0}};
    stmt.n_param = 1;
}

static auto hydrateDescribeResult(const SelectListCollector& collector, const duckdb::unique_ptr<duckdb::QueryResult>& query_result, std::vector<DescribeResult>& results) -> void {
    for (auto& row: *query_result) {
        // TODO: dealing with untype

        results.push_back({
            row.GetValue<std::string>(0),
            row.GetValue<std::string>(1),
            row.GetValue<std::string>(2) == "YES",
        });
    }
}

static auto describeSelectStatementInternal(SelectListCollector& collector, const duckdb::SelectStatement& stmt, std::vector<DescribeResult>& results) -> WorkerResultCode {
    auto& conn = collector.conn;

    std::string err_message;
    WorkerResultCode err;

    try {
        std::cout << std::format("Q: {}", stmt.ToString()) << std::endl;
        // auto prepares_stmt = std::move(conn.Prepare(stmt.Copy()));
        auto prepares_stmt = std::move(conn.Prepare(stmt.ToString()));
        // passes null value to all parameter(s)
        auto param_count = prepares_stmt->GetStatementProperties().parameter_count;
        auto params = duckdb::vector<duckdb::Value>(param_count);
        
        auto query_result = prepares_stmt->Execute(params);
        hydrateDescribeResult(collector, query_result, results);


        return no_error;
    }
    catch (const duckdb::ParameterNotResolvedException& ex) {
        err_message = "Cannot infer type for untyped placeholder";
        err = describe_filed;
    }
    catch (const duckdb::Exception& ex) {
        err_message = ex.what();
        err = invalid_sql;
    }

    collector.messageChannel("worker.describe").err(err_message);

    return err;
}

static auto describeSelectStatement(SelectListCollector& collector, duckdb::SelectStatement& stmt, std::vector<DescribeResult>& results) -> WorkerResultCode {
    prependDescribeKeyword(stmt);
    return describeSelectStatementInternal(collector, stmt, results);
}

static auto encodeDescribeResult(const std::vector<DescribeResult>& describes) -> std::vector<char> {
    CborEncoder encoder;

    encoder.addArrayHeader(describes.size());

    for (auto& desc: describes) {
        encoder.addArrayHeader(3);
        encoder.addString(desc.field_name);
        encoder.addString(desc.field_type);
        encoder.addBool(desc.nullable);
    }

    return std::move(encoder.rawBuffer());
}

auto SelectListCollector::messageChannel(const std::string& from) -> ZmqChannel {
    return ZmqChannel(this->socket, this->id, from);
}

static auto walkSQLStatement(duckdb::unique_ptr<duckdb::SQLStatement>& stmt, ZmqChannel&& zmq_channel) -> void {
    if (stmt->type == duckdb::StatementType::SELECT_STATEMENT) {
        walkSelectStatement(stmt->Cast<duckdb::SelectStatement>(), std::move(zmq_channel));
    }
    else {
        zmq_channel.warn(std::format("Unsupported statement: {}", magic_enum::enum_name(stmt->type)));
    }
}

auto SelectListCollector::execute(std::string query) -> WorkerResultCode {
    std::string message;
    try {
        auto stmts = this->conn.ExtractStatements(query);

        // for (auto& stmt: stmts) {
        if (stmts.size() > 0) {
            auto& stmt = stmts[0];
            const int32_t index = 1;
            
            walkSQLStatement(stmt, std::move(this->messageChannel("worker.parse")));
            auto stmt_copy = stmt->Copy();



            std::vector<DescribeResult> describes;

            if (stmt->type == duckdb::StatementType::SELECT_STATEMENT) {
                describeSelectStatement(*this, stmt->Cast<duckdb::SelectStatement>(), describes);
            }

            // send as worker result
            this->messageChannel("worker.bind").sendWorkerResult(index, stmts.size(), std::move(encodeDescribeResult(describes)));
        }

        return no_error;
    }
    catch (const duckdb::ParserException& ex) {
        message = ex.what();
    }
    
    this->messageChannel("worker").err(message);
    return invalid_sql;
}

extern "C" {
    auto initCollector(DatabaseRef db_ref, const char *id, size_t id_len, void *socket, CollectorRef *handle) -> int32_t {
        auto db = reinterpret_cast<worker::Database *>(db_ref);
        auto collector = new SelectListCollector(db, std::string(id, id_len), socket ? std::make_optional(socket) : std::nullopt);

        *handle = reinterpret_cast<CollectorRef>(collector);
        return 0;
    }

    auto deinitCollector(CollectorRef handle) -> void {
        delete reinterpret_cast<SelectListCollector *>(handle);
    }

    auto executeDescribe(CollectorRef handle, const char *query, size_t query_len) -> void {
        auto collector = reinterpret_cast<SelectListCollector *>(handle);

        collector->execute(std::string(query, query_len));
    }
}

}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

using namespace worker;
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
    auto sql = std::string("SELECT $1::int as p from Origin");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);
    auto stmts = std::move(collector.conn.ExtractStatements(sql));

    std::vector<DescribeResult> results;
    ::describeSelectStatement(collector, stmts[0]->Cast<duckdb::SelectStatement>(), results);

    SECTION("describe result") {
        REQUIRE(results.size() == 0);
    }
}

TEST_CASE("Describe SELECT list") {
    auto sql = std::string("SELECT CAST($1 AS INTEGER) AS a, 123, CAST($2 AS VARCHAR) AS c");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);
    auto stmts = std::move(collector.conn.ExtractStatements(sql));

    std::vector<DescribeResult> results;
    auto err = ::describeSelectStatement(collector, stmts[0]->Cast<duckdb::SelectStatement>(), results);

    SECTION("describe result") {
        SECTION("err code") {
            REQUIRE(err == no_error);
        }
        SECTION("result count") {
            REQUIRE(results.size() == 3);
        }
        SECTION("row data#1") {
            auto& row = results[0];
            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("a"));
            }
            SECTION("field type") {
                CHECK_THAT(row.field_type, Equals("INTEGER"));
            }
            SECTION("nullable") {
                CHECK(row.nullable);
            }
        }
        SECTION("row data#2") {
            auto& row = results[1];

            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("123"));
            }
            SECTION("field type") {
                CHECK_THAT(row.field_type, Equals("INTEGER"));
            }
            SECTION("nullable") {
                CHECK(row.nullable);
            }
        }
        SECTION("row data#3") {
            auto& row = results[2];

            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("c"));
            }
            SECTION("field type") {
                CHECK_THAT(row.field_type, Equals("VARCHAR"));
            }
            SECTION("nullable") {
                CHECK(row.nullable);
            }
        }
    }
}

TEST_CASE("Describe SELECT list with not null definition") {
    SKIP("(TODO) Need handling nullability manually"); 

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    collector.conn.Query("CREATE TABLE Foo (x INTEGER NOT NULL, v VARCHAR)");

    auto sql = std::string("SELECT x, v FROM Foo WHERE x = $x");
    auto stmts = std::move(collector.conn.ExtractStatements(sql));

    std::vector<DescribeResult> results;
    auto err = ::describeSelectStatement(collector, stmts[0]->Cast<duckdb::SelectStatement>(), results);

    SECTION("describe result") {
        SECTION("err code") {
            REQUIRE(err == no_error);
        }
        SECTION("result count") {
            REQUIRE(results.size() == 2);
        }
        SECTION("row data#1") {
            auto& row = results[0];

            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("x"));
            }
            SECTION("field type") {
                CHECK_THAT(row.field_type, Equals("INTEGER"));
            }
            SECTION("nullable") {
                CHECK_FALSE(row.nullable);
            }
        }
        SECTION("row data#2") {
            auto& row = results[1];

            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("v"));
            }
            SECTION("field type") {
                CHECK_THAT(row.field_type, Equals("VARCHAR"));
            }
            SECTION("nullable") {
                CHECK(row.nullable);
            }
        }
    }
}

TEST_CASE("Describe SELECT list with untyped placeholder#1") {
    auto sql = std::string(R"#(SELECT $1 || CAST($2 AS VARCHAR) AS "($a || CAST($b AS VARCHAR))", 123 as b)#");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);
    auto stmts = std::move(collector.conn.ExtractStatements(sql));

    std::vector<DescribeResult> results;
    auto err = ::describeSelectStatement(collector, stmts[0]->Cast<duckdb::SelectStatement>(), results);

    SECTION("describe result") {
        SECTION("err code") {
            REQUIRE(err == no_error);
        }
        SECTION("result count") {
            REQUIRE(results.size() == 2);
        }
        SECTION("row data#1") {
            auto& row = results[0];

            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("($a || CAST($b AS VARCHAR))"));
            }
            SECTION("field type") {
                CHECK_THAT(row.field_type, Equals("VARCHAR"));
            }
            SECTION("nullable") {
                CHECK(row.nullable);
            }
        }
        SECTION("row data#2") {
            auto& row = results[1];

            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("b"));
            }
            SECTION("field type") {
                CHECK_THAT(row.field_type, Equals("INTEGER"));
            }
            SECTION("nullable") {
                CHECK(row.nullable);
            }
        }
    }
}

TEST_CASE("Describe SELECT list with untyped placeholder#2") {
    auto sql = std::string("SELECT v || $1, 123 as b FROM Foo");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);
    collector.conn.Query("CREATE TABLE Foo (x INTEGER NOT NULL, v VARCHAR)");
    auto stmts = std::move(collector.conn.ExtractStatements(sql));

    std::vector<DescribeResult> results;
    auto err = ::describeSelectStatement(collector, stmts[0]->Cast<duckdb::SelectStatement>(), results);

    SECTION("describe result") {
        SECTION("err code") {
            REQUIRE(err == no_error);
        }
        SECTION("result count") {
            REQUIRE(results.size() == 2);
        }
        SECTION("row data#1") {
            auto& row = results[0];

            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("(v || $1)"));
            }
            SECTION("field type") {
                CHECK_THAT(row.field_type, Equals("VARCHAR"));
            }
            SECTION("nullable") {
                CHECK(row.nullable);
            }
        }
        SECTION("row data#2") {
            auto& row = results[1];

            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("b"));
            }
            SECTION("field type") {
                CHECK_THAT(row.field_type, Equals("INTEGER"));
            }
            SECTION("nullable") {
                CHECK(row.nullable);
            }
        }
    }
}
// untyped

#endif
