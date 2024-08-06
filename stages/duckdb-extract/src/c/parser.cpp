#include <optional>

#include <duckdb.hpp>
#include <duckdb/parser/query_node/list.hpp>
#include <duckdb/parser/tableref/list.hpp>
#include <duckdb/parser/expression/list.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/tableref/list.hpp>
#include <duckdb/planner/bound_parameter_map.hpp>

#define MAGIC_ENUM_RANGE_MAX (std::numeric_limits<uint8_t>::max())

#include <magic_enum/magic_enum.hpp>

#include "duckdb_worker.h"
#include "duckdb_database.hpp"
#include "duckdb_params_collector.hpp"

#include "duckdb_binder_support.hpp"
#include "zmq_worker_support.hpp"
#include "cbor_encode.hpp"

#include <iostream>

namespace worker {

class DescribeWorker {
public:
    duckdb::Connection conn;
public:
    DescribeWorker(worker::Database *db, std::string id, std::optional<void *> socket) 
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

static auto hydrateDescribeResult(const DescribeWorker& collector, const duckdb::unique_ptr<duckdb::QueryResult>& query_result, std::vector<DescribeResult>& results) -> void {
    for (auto& row: *query_result) {
        // TODO: dealing with untype

        results.push_back({
            row.GetValue<std::string>(0),
            row.GetValue<std::string>(1),
            row.GetValue<std::string>(2) == "YES",
        });
    }
}

static auto describeSelectStatementInternal(DescribeWorker& collector, const duckdb::SelectStatement& stmt, std::vector<DescribeResult>& results) -> WorkerResultCode {
    auto& conn = collector.conn;

    std::string err_message;
    WorkerResultCode err;

    try {
        // std::cout << std::format("Q: {}", stmt.ToString()) << std::endl;
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

static auto describeSelectStatement(DescribeWorker& collector, duckdb::SelectStatement& stmt, std::vector<DescribeResult>& results) -> WorkerResultCode {
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

static auto walkSQLStatement(duckdb::unique_ptr<duckdb::SQLStatement>& stmt, ZmqChannel& channel) -> ParameterCollector::Result {
    ParameterCollector collector(evalParameterType(stmt), std::forward<ZmqChannel>(channel));

    if (stmt->type == duckdb::StatementType::SELECT_STATEMENT) {
        return collector.walkSelectStatement(stmt->Cast<duckdb::SelectStatement>());
    }
    else {
        collector.channel.warn(std::format("Unsupported statement: {}", magic_enum::enum_name(stmt->type)));

        return {.type = StatementType::Invalid, .lookup{}};
    }
}

auto bindTypeToTableRef(duckdb::ClientContext& context, duckdb::unique_ptr<duckdb::SQLStatement>&& stmt, StatementType type) -> duckdb::unique_ptr<duckdb::BoundTableRef> {
    if (type != StatementType::Select) {
        return nullptr;
    }

    auto& node = stmt->Cast<duckdb::SelectStatement>().node;

    if (node->type != duckdb::QueryNodeType::SELECT_NODE) {
        return nullptr;
    }

    duckdb::case_insensitive_map_t<duckdb::BoundParameterData> parameter_map{};
    duckdb::BoundParameterMap parameters(parameter_map);
    
    auto binder = duckdb::Binder::CreateBinder(context);

    binder->SetCanContainNulls(true);
    binder->parameters = &parameters;

    return std::move(binder->Bind(*node->Cast<duckdb::SelectNode>().from_table));
}

auto bindTypeToStatement(duckdb::ClientContext& context, duckdb::unique_ptr<duckdb::SQLStatement>&& stmt) -> duckdb::BoundStatement {
    duckdb::case_insensitive_map_t<duckdb::BoundParameterData> parameter_map{};
    duckdb::BoundParameterMap parameters(parameter_map);
    
    auto binder = duckdb::Binder::CreateBinder(context);

    binder->SetCanContainNulls(true);
    binder->parameters = &parameters;

    return std::move(binder->Bind(*stmt));
}

auto DescribeWorker::messageChannel(const std::string& from) -> ZmqChannel {
    return ZmqChannel(this->socket, this->id, from);
}

auto DescribeWorker::execute(std::string query) -> WorkerResultCode {
    std::string message;
    try {
        auto stmts = this->conn.ExtractStatements(query);
        auto zmq_channel = this->messageChannel("worker.parse");

        // for (auto& stmt: stmts) {
        if (stmts.size() > 0) {
            auto& stmt = stmts[0];
            const int32_t stmt_offset = 1;
            const int32_t stmt_size = 1;
            
            auto param_result = walkSQLStatement(stmt, zmq_channel);
            auto q = stmt->ToString();
            
            std::vector<ParamEntry> param_type_result;
            std::vector<ColumnEntry> column_type_result;
            try {
                this->conn.BeginTransaction();

                auto bound_stmt = bindTypeToStatement(*this->conn.context, stmt->Copy());
                auto bound_table = bindTypeToTableRef(*this->conn.context, stmt->Copy(), param_result.type);

                param_type_result = resolveParamType(bound_stmt.plan, param_result.lookup);
                column_type_result = resolveColumnType(bound_stmt.plan, bound_table, param_result.type);

                this->conn.Commit();
            }
            catch (...) {
                this->conn.Rollback();
                throw;
            }

            send_query: {
                zmq_channel.sendWorkerResult(stmt_offset, stmt_size, topic_query, std::vector<char>(q.cbegin(), q.cend()));
            }
            placeholder: {
                // TODO: zmq_channel.sendWorkerResult(stmt_offset, stmt_size, topic_placeholder, encodePlaceholder(param_type_result));
            }
            select_list: {
                // TODO: zmq_channel.sendWorkerResult(stmt_offset, stmt_size, topic_select_list, encodeSelectList(column_type_result));
            }

            auto stmt_copy = stmt->Copy();
            std::vector<DescribeResult> describes;

            if (stmt_copy->type == duckdb::StatementType::SELECT_STATEMENT) {
                describeSelectStatement(*this, stmt_copy->Cast<duckdb::SelectStatement>(), describes);
            }

            // send as worker result
            this->messageChannel("worker.bind").sendWorkerResult(stmt_offset, stmts.size(), topic_select_list, std::move(encodeDescribeResult(describes)));
        }

        return no_error;
    }
    catch (const duckdb::ParserException& ex) {
        message = ex.what();
    }
    catch (const duckdb::Exception& ex) {
        message = ex.what();
    }
    
    this->messageChannel("worker").err(message);
    return invalid_sql;
}

extern "C" {
    auto initCollector(DatabaseRef db_ref, const char *id, size_t id_len, void *socket, CollectorRef *handle) -> int32_t {
        auto db = reinterpret_cast<worker::Database *>(db_ref);
        auto collector = new DescribeWorker(db, std::string(id, id_len), socket ? std::make_optional(socket) : std::nullopt);

        *handle = reinterpret_cast<CollectorRef>(collector);
        return 0;
    }

    auto deinitCollector(CollectorRef handle) -> void {
        delete reinterpret_cast<DescribeWorker *>(handle);
    }

    auto executeDescribe(CollectorRef handle, const char *query, size_t query_len) -> void {
        auto collector = reinterpret_cast<DescribeWorker *>(handle);

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

// TEST_CASE("Error SQL") {
//     auto sql = std::string("SELT $1::int as a");

//     auto db = worker::Database();
//     auto collector = DescribeWorker(&db, "1", std::nullopt);
    
//     auto err = collector.execute(sql);

//     SECTION("execute result code") {
//         CHECK(err != 0);
//     }
// }

// TEST_CASE("Prepend describe") {
//     auto sql = std::string("select $1::int as a, xyz, 123, $2::text as c from Foo");

//     auto db = worker::Database();
//     auto collector = DescribeWorker(&db, "1", std::nullopt);

//     auto conn = db.connect();;
//     auto stmts = conn.ExtractStatements(sql);
//     auto& stmt = stmts[0];
//     ::prependDescribeKeyword(stmt->Cast<duckdb::SelectStatement>());

//     SECTION("result") {
//         CHECK_THAT(stmt->ToString(), Equals("DESCRIBE (SELECT CAST($1 AS INTEGER) AS a, xyz, 123, CAST($2 AS VARCHAR) AS c FROM Foo)"));
//     }
// }

// TEST_CASE("Not exist relation") {
//     auto sql = std::string("SELECT $1::int as p from Origin");

//     auto db = worker::Database();
//     auto collector = DescribeWorker(&db, "1", std::nullopt);
//     auto stmts = std::move(collector.conn.ExtractStatements(sql));

//     std::vector<DescribeResult> results;
//     ::describeSelectStatement(collector, stmts[0]->Cast<duckdb::SelectStatement>(), results);

//     SECTION("describe result") {
//         REQUIRE(results.size() == 0);
//     }
// }

#endif
