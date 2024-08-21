#include <optional>
#include <iostream>

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

namespace worker {

class DescribeWorker {
public:
    duckdb::Connection conn;
public:
    DescribeWorker(worker::Database *db, std::string&& id, std::optional<void *>&& socket) 
        : conn(std::move(db->connect())), id(id), socket(socket) {}
public:
    auto execute(std::string query) -> WorkerResultCode;
public:
    auto messageChannel(const std::string& from) -> ZmqChannel;
private:
    std::string id;
    std::optional<void *> socket;
};

auto walkSQLStatement(duckdb::unique_ptr<duckdb::SQLStatement>& stmt, ZmqChannel&& channel) -> ParamCollectionResult {
    ParameterCollector collector(evalParameterType(stmt), std::forward<ZmqChannel>(channel));

    if (stmt->type == duckdb::StatementType::SELECT_STATEMENT) {
        return collector.walkSelectStatement(stmt->Cast<duckdb::SelectStatement>());
    }
    else {
        collector.channel.warn(std::format("Unsupported statement: {}", magic_enum::enum_name(stmt->type)));

        return {.type = StatementType::Invalid, .names{}, .param_user_types{}};
    }
}

auto bindTypeToStatement(duckdb::ClientContext& context, duckdb::unique_ptr<duckdb::SQLStatement>&& stmt) -> duckdb::BoundStatement {
    duckdb::case_insensitive_map_t<duckdb::BoundParameterData> parameter_map{};
    duckdb::BoundParameterMap parameters(parameter_map);
    
    auto binder = duckdb::Binder::CreateBinder(context);

    binder->SetCanContainNulls(true);
    binder->parameters = &parameters;

    return std::move(binder->Bind(*stmt));
}

static auto encodePlaceholder(std::vector<ParamEntry>&& entries) -> std::vector<char> {
    std::ranges::sort(entries, {}, &ParamEntry::sort_order);

    CborEncoder encoder;

    encoder.addArrayHeader(entries.size());

    for (auto& entry: entries) {
        encoder.addArrayHeader(3);
        encoder.addString(entry.name);

        if (entry.type_name) {
            encoder.addString(entry.type_name.value());
        }
        else {
            encoder.addNull();
        }
    }

    return std::move(encoder.rawBuffer());
}

static auto encodeSelectList(std::vector<ColumnEntry>&& entries) -> std::vector<char> {
    CborEncoder encoder;

    encoder.addArrayHeader(entries.size());

    for (auto& entry: entries) {
        encoder.addArrayHeader(3);
        encoder.addString(entry.field_name);
        encoder.addString(entry.field_type);
        encoder.addBool(entry.nullable);
    }

    return std::move(encoder.rawBuffer());
}

static auto encodeBoundUserType(UserTypeLookup<PositionalParam>&& param_user_types, UserTypeLookup<duckdb::idx_t>&& sel_list_user_types) -> std::vector<char> {
    auto user_types_view_1 = param_user_types | std::views::values;
    auto user_types_view_2 = sel_list_user_types | std::views::values;
    
    std::unordered_set<std::string> user_types;
    std::ranges::move(user_types_view_1.begin(), user_types_view_1.end(), std::inserter(user_types, user_types.end()));
    std::ranges::move(user_types_view_2.begin(), user_types_view_2.end(), std::inserter(user_types, user_types.end()));

    CborEncoder encoder;

    encoder.addArrayHeader(user_types.size());

    for (auto& name: user_types) {
        encoder.addString(name);
    }

    return std::move(encoder.rawBuffer());
}

auto DescribeWorker::messageChannel(const std::string& from) -> ZmqChannel {
    return ZmqChannel(this->socket, this->id, from);
}

auto DescribeWorker::execute(std::string query) -> WorkerResultCode {
    std::string message;
    try {
        auto stmts = this->conn.ExtractStatements(query);

        // for (auto& stmt: stmts) {
        if (stmts.size() > 0) {
            auto& stmt = stmts[0];
            const int32_t stmt_offset = 1;
            const int32_t stmt_size = 1;
            
            auto param_result = walkSQLStatement(stmt, this->messageChannel("worker.parse"));
            auto q = stmt->ToString();
            
            ParamResolveResult param_type_result;
            std::vector<ColumnEntry> column_type_result;
            try {
                this->conn.BeginTransaction();

                auto bound_stmt = bindTypeToStatement(*this->conn.context, stmt->Copy());

                param_type_result = resolveParamType(bound_stmt.plan, std::move(param_result.names), param_result.param_user_types);
                auto channel = this->messageChannel("worker.parse");
                column_type_result = resolveColumnType(bound_stmt.plan, param_result.type, channel);

                this->conn.Commit();
            }
            catch (...) {
                this->conn.Rollback();
                throw;
            }

            auto zmq_channel = this->messageChannel("worker.extract");

            std::unordered_map<std::string, std::vector<char>> topic_bodies({
                {topic_query, std::vector<char>(q.cbegin(), q.cend())},
                {topic_placeholder, encodePlaceholder(std::move(param_type_result.params))},
                {topic_select_list, encodeSelectList(std::move(column_type_result))},
                {bound_user_type, encodeBoundUserType(std::move(param_result.param_user_types), std::move(param_result.sel_list_user_types))}
            });
            zmq_channel.sendWorkerResult(stmt_offset, stmt_size, topic_bodies);
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
    auto initSourceCollector(DatabaseRef db_ref, const char *id, size_t id_len, void *socket, CollectorRef *handle) -> int32_t {
        auto db = reinterpret_cast<worker::Database *>(db_ref);
        auto collector = new DescribeWorker(db, std::string(id, id_len), socket ? std::make_optional(socket) : std::nullopt);

        *handle = reinterpret_cast<CollectorRef>(collector);
        return 0;
    }

    auto deinitSourceCollector(CollectorRef handle) -> void {
        delete reinterpret_cast<DescribeWorker *>(handle);
    }

    auto executeDescribe(CollectorRef handle, const char *query, size_t query_len) -> WorkerResultCode {
        auto collector = reinterpret_cast<DescribeWorker *>(handle);

        return collector->execute(std::string(query, query_len));
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

#endif
