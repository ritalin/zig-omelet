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

        return {.type = StatementType::Invalid, .names{}};
    }
}

static auto bindParamTypeHint(duckdb::Binder& binder, const ParamNameLookup& names) -> BoundParamTypeHint {
    BoundParamTypeHint result;
    duckdb::ExpressionBinder expr_binder(binder, binder.context);

    for (auto& [key, entry]: names) {
        if (entry.type_hint) {
            auto expr = entry.type_hint->Copy();
            result.insert({key, expr_binder.Bind(expr)});
        }
    }

    return result;
}

auto bindTypeToStatement(
    duckdb::ClientContext& context, 
    duckdb::unique_ptr<duckdb::SQLStatement>&& stmt, 
    const ParamNameLookup& names, 
    const ParamExampleLookup& examples) -> BoundResult 
{
    auto example_view = examples | std::views::transform([](const auto& pair) {
        return std::make_pair(pair.first, duckdb::BoundParameterData(pair.second.value));
    });

    duckdb::case_insensitive_map_t<duckdb::BoundParameterData> parameter_map(example_view.begin(), example_view.end());
    duckdb::BoundParameterMap parameters(parameter_map);
    
    auto binder = duckdb::Binder::CreateBinder(context);

    binder->SetCanContainNulls(true);
    binder->parameters = &parameters;

    return {
        .stmt = std::move(binder->Bind(*stmt)),
        .type_hints = bindParamTypeHint(*binder, names),
    };
}

static auto encodePlaceholder(std::vector<ParamEntry>& entries) -> std::vector<char> {
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

static auto encodePlaceholderOrder(std::vector<ParamEntry>& entries) -> std::vector<char> {
    CborEncoder encoder;

    encoder.addArrayHeader(entries.size());

    for (auto& entry: entries) {
        encoder.addString(entry.name);
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

static auto encodeBoundUserType(std::vector<std::string>&& param_user_types, std::vector<std::string>&& sel_list_user_types) -> std::vector<char> {
    std::unordered_set<std::string> user_types;
    std::ranges::move(param_user_types.begin(), param_user_types.end(), std::inserter(user_types, user_types.end()));
    std::ranges::move(sel_list_user_types.begin(), sel_list_user_types.end(), std::inserter(user_types, user_types.end()));

    CborEncoder encoder;

    encoder.addArrayHeader(user_types.size());

    for (auto& name: user_types) {
        encoder.addString(name);
    }

    return std::move(encoder.rawBuffer());
}

static auto encodeAnonymousUserType(std::vector<UserTypeEntry>&& param_anon_types, std::vector<UserTypeEntry>&& sel_list_anon_types) -> std::vector<char> {
    CborEncoder encoder;

    encoder.addArrayHeader(param_anon_types.size() + sel_list_anon_types.size());

    for (auto& entry: param_anon_types) {
        encodeUserType(encoder, entry);
    }
    for (auto& entry: sel_list_anon_types) {
        encodeUserType(encoder, entry);
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
            
            auto walk_result = walkSQLStatement(stmt, this->messageChannel("worker.parse"));
            auto q = stmt->ToString();
            
            ParamResolveResult param_type_result;
            ColumnResolveResult column_type_result;
            try {
                this->conn.BeginTransaction();

                auto bound_result = bindTypeToStatement(*this->conn.context, stmt->Copy(), walk_result.names, walk_result.examples);

                param_type_result = resolveParamType(bound_result.stmt.plan, std::move(walk_result.names), std::move(bound_result.type_hints), std::move(walk_result.examples));
                auto channel = this->messageChannel("worker.parse");
                column_type_result = resolveColumnType(bound_result.stmt.plan, walk_result.type, channel);

                this->conn.Commit();
            }
            catch (...) {
                this->conn.Rollback();
                throw;
            }

            auto zmq_channel = this->messageChannel("worker.extract");

            std::ranges::sort(param_type_result.params, {}, &ParamEntry::sort_order);

            std::unordered_map<std::string, std::vector<char>> topic_bodies({
                {topic_query, std::vector<char>(q.cbegin(), q.cend())},
                {topic_anon_user_type, encodeAnonymousUserType(std::move(param_type_result.anon_types), std::move(column_type_result.anon_types))},
                {topic_placeholder, encodePlaceholder(param_type_result.params)},
                {topic_placeholder_order, encodePlaceholderOrder(param_type_result.params)},
                {topic_select_list, encodeSelectList(std::move(column_type_result.columns))},
                {topic_bound_user_type, encodeBoundUserType(std::move(param_type_result.user_type_names), std::move(column_type_result.user_type_names))},
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
