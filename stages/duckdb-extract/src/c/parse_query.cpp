#include <optional>
#include <iostream>

#include <duckdb.hpp>
#include <duckdb/parser/statement/select_statement.hpp>
#include <duckdb/parser/statement/delete_statement.hpp>
#include <duckdb/parser/statement/update_statement.hpp>
#include <duckdb/parser/statement/insert_statement.hpp>
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
#include "response_encode_support.hpp"

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
    auto messageChannel(const std::optional<size_t>& offset, const std::string& from) -> ZmqChannel;
private:
    std::string id;
    std::optional<void *> socket;
};

auto walkSQLStatement(duckdb::unique_ptr<duckdb::SQLStatement>& stmt, ZmqChannel&& channel) -> ResolveResult<ParamCollectionResult> {
    ParameterCollector collector(evalParameterType(stmt), std::forward<ZmqChannel>(channel));

    switch (stmt->type) {
    case duckdb::StatementType::SELECT_STATEMENT: 
        {
            return {.data = collector.walkSelectStatement(stmt->Cast<duckdb::SelectStatement>()), .handled = ResolveStatus::Handled};
        }
    case duckdb::StatementType::DELETE_STATEMENT: 
        {
            return {.data = collector.walkDeleteStatement(stmt->Cast<duckdb::DeleteStatement>()), .handled = ResolveStatus::Handled};
        }
    case duckdb::StatementType::UPDATE_STATEMENT: 
        {
            return {.data = collector.walkUpdateStatement(stmt->Cast<duckdb::UpdateStatement>()), .handled = ResolveStatus::Handled};
        }
    case duckdb::StatementType::INSERT_STATEMENT: 
        {
            return {.data = collector.walkInsertStatement(stmt->Cast<duckdb::InsertStatement>()), .handled = ResolveStatus::Handled};
        }
    default: 
        {
            collector.channel.warn(std::format("Unsupported statement: {}", magic_enum::enum_name(stmt->type)));
            return {.data = {.type = StatementType::Invalid, .names{}}, .handled = ResolveStatus::Unhandled};
        }
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
        .stmt = binder->Bind(*stmt),
        .type_hints = bindParamTypeHint(*binder, names),
    };
}

static auto encodePlaceholder(std::vector<ParamEntry>& entries) -> std::vector<char> {
    CborEncoder encoder;

    encoder.addArrayHeader(entries.size());

    for (auto& entry: entries) {
        encoder.addArrayHeader(3);
        encoder.addString(entry.name);
        encoder.addUInt(static_cast<uint64_t>(entry.type_kind));

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
        encoder.addArrayHeader(4);
        encoder.addString(entry.field_name);
        encoder.addUInt(static_cast<uint64_t>(entry.type_kind));
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

auto DescribeWorker::messageChannel(const std::optional<size_t>& offset, const std::string& from) -> ZmqChannel {
    return ZmqChannel(this->socket, offset, this->id, from);
}

static auto parseQuery(duckdb::Connection& conn, std::string query, ZmqChannel&& channel) -> std::vector<duckdb::unique_ptr<duckdb::SQLStatement>> {
    if (query == "") {
        channel.warn("Cannot handle an empty query");
        channel.sendWorkerResponse(::worker_skipped, encodeStatementOffset(0));
        return {};
    }
    
    std::string message;

    try {
        auto stmts = conn.ExtractStatements(query);

        channel.sendWorkerResponse(::worker_progress, encodeStatementCount(stmts.size()));

        return std::move(stmts);
    }
    catch (const duckdb::ParserException& ex) {
        message = ex.what();
    }

    channel.err(message);
    channel.sendWorkerResponse(::worker_skipped, encodeStatementOffset(0));

    return {};
}

static auto executeInternal(duckdb::Connection& conn, const size_t stmt_offset, duckdb::unique_ptr<duckdb::SQLStatement>& stmt, ZmqChannel&& channel) -> void {
    std::string message;
    try {
        auto walk_result = walkSQLStatement(stmt, channel.clone());
        if (walk_result.handled == ResolveStatus::Unhandled) {
            channel.sendWorkerResponse(::worker_skipped, encodeStatementOffset(stmt_offset));
            return;
        }

        auto q = stmt->ToString();
        
        ParamResolveResult param_type_result;
        ColumnResolveResult column_type_result;
        try {
            conn.BeginTransaction();

            auto bound_result = bindTypeToStatement(*conn.context, stmt->Copy(), walk_result.data.names, walk_result.data.examples);

            auto channel_extract = channel.clone();
            param_type_result = resolveParamType(bound_result.stmt.plan, std::move(walk_result.data.names), std::move(bound_result.type_hints), std::move(walk_result.data.examples), channel);
            column_type_result = resolveColumnType(bound_result.stmt.plan, walk_result.data.type, conn, channel);

            conn.Commit();
        }
        catch (...) {
            conn.Rollback();
            throw;
        }

        std::ranges::sort(param_type_result.params, {}, &ParamEntry::sort_order);

        std::unordered_map<std::string, std::vector<char>> topic_bodies({
            {topic_query, std::vector<char>(q.cbegin(), q.cend())},
            {topic_anon_user_type, encodeAnonymousUserType(std::move(param_type_result.anon_types), std::move(column_type_result.anon_types))},
            {topic_placeholder, encodePlaceholder(param_type_result.params)},
            {topic_placeholder_order, encodePlaceholderOrder(param_type_result.params)},
            {topic_select_list, encodeSelectList(std::move(column_type_result.columns))},
            {topic_bound_user_type, encodeBoundUserType(std::move(param_type_result.user_type_names), std::move(column_type_result.user_type_names))},
        });
        channel.clone().sendWorkerResponse(
            ::worker_result, 
            encodeTopicBody(stmt_offset, topic_bodies)
        );

        return;
    }
    catch (const duckdb::Exception& ex) {
        message = ex.what();
    }
    catch (...) {
        message = "Unexpected error";
    }
    
    channel.err(message);
}

auto DescribeWorker::execute(std::string query) -> WorkerResultCode {
    auto stmts = parseQuery(this->conn, query, this->messageChannel(std::nullopt, "worker.phase.parse"));

    const size_t stmt_offset = 0;

    // for (auto& stmt: stmts) {
    if (stmts.size() > 0) {
        executeInternal(this->conn, stmt_offset, stmts[stmt_offset], this->messageChannel(stmt_offset, "worker.phase.extract"));
    }

    this->messageChannel(std::nullopt, "worker.phase.done").sendWorkerResponse(::worker_finished, {});

    return no_error;
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
