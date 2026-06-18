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
#include "worker_support.hpp"
#include "cbor_encode.hpp"
#include "response_encode_support.hpp"

namespace worker {

class DescribeWorker {
public:
    duckdb::Connection conn;
    std::vector<nng_msg*> results;
public:
    DescribeWorker(worker::Database *db, Slice&& from_stage, SourceDescriptor&& desc): 
        conn(std::move(db->connect())), 
        from_stage({from_stage.ptr, from_stage.len}),
        desc(desc)
    {
    }
public:
    auto execute(std::string query) -> WorkerResultCode;
    auto messageChannel(const std::optional<size_t>& offset, std::string&& phase) -> NngChannel;
    auto name() const -> std::string_view;
    auto rename(std::string_view base_name, const size_t stmt_index, const size_t stmt_count) -> std::optional<std::string>;
private:
    std::string_view from_stage;
    SourceDescriptor desc;
};

auto walkSQLStatement(duckdb::unique_ptr<duckdb::SQLStatement>& stmt, NngChannel& channel) -> ResolveResult<ParamCollectionResult> {
    ParameterCollector collector(evalParameterType(stmt), channel);

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
            channel.warn(std::format("Unsupported statement: {}", magic_enum::enum_name(stmt->type)));
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
    binder->SetParameters(parameters);
    
    return {
        .stmt = binder->Bind(*stmt),
        .type_hints = bindParamTypeHint(*binder, names),
    };
}

static auto encodePlaceholder(std::vector<ParamEntry>& entries) -> CborEncoder<VectorBackend> {
    auto encoder = CborEncoder(VectorBackend());

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

    encoder.flush();

    return std::move(encoder);
}

static auto encodePlaceholderOrder(std::vector<ParamEntry>& entries) -> CborEncoder<VectorBackend> {
    auto encoder = CborEncoder(VectorBackend());

    encoder.addArrayHeader(entries.size());

    for (auto& entry: entries) {
        encoder.addString(entry.name);
    }

    encoder.flush();

    return std::move(encoder);
}

static auto encodeSelectList(std::vector<ColumnEntry>&& entries) -> CborEncoder<VectorBackend> {
    auto encoder = CborEncoder(VectorBackend());

    encoder.addArrayHeader(entries.size());

    for (auto& entry: entries) {
        encoder.addArrayHeader(4);
        encoder.addString(entry.field_name);
        encoder.addUInt(static_cast<uint64_t>(entry.type_kind));
        encoder.addString(entry.field_type);
        encoder.addBool(entry.nullable);
    }

    encoder.flush();

    return std::move(encoder);
}

static auto encodeBoundUserType(std::vector<std::string>&& param_user_types, std::vector<std::string>&& sel_list_user_types) -> CborEncoder<VectorBackend> {
    std::unordered_set<std::string> user_types;
    std::ranges::move(param_user_types.begin(), param_user_types.end(), std::inserter(user_types, user_types.end()));
    std::ranges::move(sel_list_user_types.begin(), sel_list_user_types.end(), std::inserter(user_types, user_types.end()));

    auto encoder = CborEncoder(VectorBackend());

    encoder.addArrayHeader(user_types.size());

    for (auto& name: user_types) {
        encoder.addString(name);
    }

    encoder.flush();

    return std::move(encoder);
}

static auto encodeQuery(const std::string& query) -> CborEncoder<VectorBackend> {
    auto encoder = CborEncoder(VectorBackend());
    encoder.addString(query);
    encoder.flush();

    return std::move(encoder);
}

static auto encodeAnonymousUserType(std::vector<UserTypeEntry>&& param_anon_types, std::vector<UserTypeEntry>&& sel_list_anon_types) -> CborEncoder<VectorBackend> {
    auto encoder = CborEncoder(VectorBackend());

    encoder.addArrayHeader(param_anon_types.size() + sel_list_anon_types.size());

    for (auto& entry: param_anon_types) {
        encodeUserType(encoder, entry);
    }
    for (auto& entry: sel_list_anon_types) {
        encodeUserType(encoder, entry);
    }

    encoder.flush();

    return std::move(encoder);
}

auto DescribeWorker::messageChannel(const std::optional<size_t>& offset, std::string&& phase) -> NngChannel {
    return NngChannel(this->desc, offset, this->from_stage, phase);
}

auto DescribeWorker::name() const -> std::string_view {
    return std::string_view(this->desc.name.ptr, this->desc.name.len);
}

auto DescribeWorker::rename(std::string_view base_name, const size_t stmt_index, const size_t stmt_count) -> std::optional<std::string> {
    return stmt_count > 1 ? std::make_optional(std::format("{}_{}", base_name, stmt_index+1)) : std::nullopt;
}

static auto parseQuery(duckdb::Connection& conn, std::string query, NngChannel& channel) -> std::vector<duckdb::unique_ptr<duckdb::SQLStatement>> {
    std::string message;

    try {
        auto stmts = conn.ExtractStatements(query);
        if (stmts.size() == 0) {
            channel.warn("Cannot handle an empty query");
            channel.makeWorkerResponse([](auto& encoder, auto& stage, auto& desc, auto) {
                // TODO:
                // ::worker_skipped, );
                encodeStatementOffset(encoder, stage, desc, 0);
            });
            return {};
        }
    
        // TODO: deprecated
        // channel.sendWorkerResponse([&](auto& encoder, auto& offset, auto& stage) {
        //     // TODO:
        //     // ::worker_progress, 
        //     encodeStatementCount(encoder, stage, stmts.size());
        // });

        return std::move(stmts);
    }
    catch (const duckdb::Exception& ex) {
        message = ex.what();
    }

    channel.err(message);
    channel.makeWorkerResponse([](auto& encoder, auto& stage, auto& desc, auto) {
        encodeStatementOffset(encoder, stage, desc, 0);
    });

    return {};
}

static auto executeInternal(duckdb::Connection& conn, duckdb::unique_ptr<duckdb::SQLStatement>& stmt, std::optional<std::string>&& stmt_name, NngChannel& channel) -> void {
    std::string message;
    try {
        auto walk_result = walkSQLStatement(stmt, channel);
        if (walk_result.handled == ResolveStatus::Unhandled) {
            channel.makeWorkerResponse([](auto& encoder, auto& stage, auto& desc, auto offset) {
                encodeStatementOffset(encoder, stage, desc, offset);
            });
            return;
        }

        auto q = stmt->ToString();
        
        ParamResolveResult param_type_result;
        ColumnResolveResult column_type_result;
        try {
            conn.BeginTransaction();

            auto bound_result = bindTypeToStatement(*conn.context, stmt->Copy(), walk_result.data.names, walk_result.data.examples);

            param_type_result = resolveParamType(bound_result.stmt.plan, std::move(walk_result.data.names), std::move(bound_result.type_hints), std::move(walk_result.data.examples), channel);
            column_type_result = resolveColumnType(bound_result.stmt.plan, walk_result.data.type, conn, channel);

            conn.Commit();
        }
        catch (...) {
            conn.Rollback();
            throw;
        }

        std::ranges::sort(param_type_result.params, {}, &ParamEntry::sort_order);

        std::unordered_map<std::string_view, CborEncoder<VectorBackend>> topic_bodies;
        {
            topic_bodies.emplace(topic_query, encodeQuery(q));
            topic_bodies.emplace(topic_anon_user_type, encodeAnonymousUserType(std::move(param_type_result.anon_types), std::move(column_type_result.anon_types)));
            topic_bodies.emplace(topic_placeholder, encodePlaceholder(param_type_result.params));
            topic_bodies.emplace(topic_placeholder_order, encodePlaceholderOrder(param_type_result.params));
            topic_bodies.emplace(topic_select_list, encodeSelectList(std::move(column_type_result.columns)));
            topic_bodies.emplace(topic_bound_user_type, encodeBoundUserType(std::move(param_type_result.user_type_names), std::move(column_type_result.user_type_names)));
        }
        channel.makeWorkerResponse([&](auto& encoder, auto& stage, auto& desc, auto offset){
            encodeTopicBody(encoder, stage, desc, offset, stmt_name, topic_bodies);
        });

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
    auto parse_channel = this->messageChannel(std::nullopt, "parse");
    auto stmts = parseQuery(this->conn, query, parse_channel);
    parse_channel.collectInto(this->results);

    for (size_t stmt_offset = 0; auto& stmt: stmts) {
        auto channel = this->messageChannel(stmt_offset, "extract");
        executeInternal(
            this->conn, stmt, 
            this->rename(std::string(this->name()), stmt_offset, stmts.size()),
            channel
        );
        channel.collectInto(this->results);
        ++stmt_offset;
    }

    return no_error;
}

extern "C" {
    auto initSourceCollector(DatabaseRef db_ref, Slice stage, SourceDescriptor desc, CollectorRef *handle) -> int32_t {
        auto db = reinterpret_cast<worker::Database *>(db_ref);
        auto collector = new DescribeWorker(db, std::move(stage), std::move(desc));

        *handle = reinterpret_cast<CollectorRef>(collector);
        return 0;
    }

    auto deinitSourceCollector(CollectorRef handle) -> void {
        delete reinterpret_cast<DescribeWorker *>(handle);
    }

    auto executeDescribe(CollectorRef handle, Slice query) -> WorkerResultCode {
        auto collector = reinterpret_cast<DescribeWorker *>(handle);

        return collector->execute(std::string(query.ptr, query.len));
    }

    auto getDescribeResultCount(CollectorRef handle) -> size_t {
        auto collector = reinterpret_cast<DescribeWorker *>(handle);

        return collector->results.size();
    }
    
    auto getDescribeResult(CollectorRef handle, size_t index) -> nng_msg* {
        auto collector = reinterpret_cast<DescribeWorker *>(handle);

        return collector->results[index];
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
