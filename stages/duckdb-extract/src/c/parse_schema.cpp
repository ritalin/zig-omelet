#include <duckdb.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/parser/parsed_data/create_type_info.hpp>

#include <magic_enum/magic_enum.hpp>

#include "duckdb_worker.h"
#include "duckdb_database.hpp"
#include "zmq_worker_support.hpp"
#include "cbor_encode.hpp"
#include "duckdb_binder_support.hpp"
#include "response_encode_support.hpp"

namespace worker {

class UserTypeWorker {
public:
    std::vector<nng_msg*> results;
public:
    UserTypeWorker(worker::Database *db, Slice&& from_stage, SourceDescriptor&& desc): 
        conn(db->connect()), 
        from_stage(std::string_view(from_stage.ptr, from_stage.len)),
        desc(desc),
        results({})
    {
    }
public:
    auto execute(std::string&& query) -> WorkerResultCode;
    auto messageChannel(const std::optional<size_t>& offset, std::string_view phase) -> NngChannel;
    auto rename(std::string&& base_name, const size_t stmt_index, const size_t stmt_count) -> std::optional<std::string>;
private:
    duckdb::Connection conn;
    std::string_view from_stage;
    SourceDescriptor desc;
};

static auto encodeUserType(const UserTypeEntry& entry) -> CborEncoder<VectorBackend> {
    auto encoder = CborEncoder(VectorBackend());
    encodeUserType(encoder, entry);
    encoder.flush();

    return std::move(encoder);
}

static auto encodeBoundUserType(std::vector<std::string>&& user_types) -> CborEncoder<VectorBackend> {
    auto encoder = CborEncoder(VectorBackend());

    encoder.addArrayHeader(user_types.size());

    for (auto& name: user_types) {
        encoder.addString(name);
    }

    encoder.flush();

    return std::move(encoder);
}

static auto encodeAnonymousUserType(std::vector<UserTypeEntry>&& anon_types) -> CborEncoder<VectorBackend> {
    auto encoder = CborEncoder(VectorBackend());

    encoder.addArrayHeader(anon_types.size());

    for (auto& entry: anon_types) {
        encodeUserType(encoder, entry);
    }

    encoder.flush();

    return std::move(encoder);
}

static auto pickUserTypeName(const duckdb::unique_ptr<duckdb::SQLStatement>& stmt) -> std::string {
    switch (stmt->type) {
    case duckdb::StatementType::CREATE_STATEMENT: 
        {
            auto& create_stmt = stmt->Cast<duckdb::CreateStatement>();
            switch (create_stmt.info->type) {
            case duckdb::CatalogType::TYPE_ENTRY:
                {
                    auto& type_entry = create_stmt.info->Cast<duckdb::CreateTypeInfo>();
                    return type_entry.name;
                }   
            default: {}
            }
        }
        break;
    default: {}
    }
        
    return std::format("_unsupported_{}", magic_enum::enum_name(stmt->type));
}

static auto parseQuery(duckdb::Connection& conn, std::string query, NngChannel& channel) -> std::vector<duckdb::unique_ptr<duckdb::SQLStatement>> {    
    std::string message;

    try {
        auto stmts = conn.ExtractStatements(query);
        if (stmts.size() == 0) {
            channel.warn("Cannot handle an empty schema");
            channel.makeWorkerResponse([](auto& encoder, auto& worker_phase, auto& desc, auto) {
                encodeStatementOffset(encoder, worker_phase, desc, 0);
            });
            return {};
        }

        // TODO:deprecated
        // channel.makeWorkerResponse([](auto& encoder, auto& worker_phase, auto& desc, auto& offset) {
        //     ::worker_progress, encodeStatementCount(stmts.size());
        // });

        return std::move(stmts);
    }
    catch (const duckdb::Exception& ex) {
        message = ex.what();
    }

    channel.err(message);
    channel.makeWorkerResponse([](auto& encoder, auto& worker_phase, auto& desc, auto) {
        encodeStatementOffset(encoder, worker_phase, desc, 0);
    });

    return {};
}

static auto isSupportedStatements(duckdb::unique_ptr<duckdb::SQLStatement>& stmt, NngChannel& channel) -> bool {
    if (stmt->type != duckdb::StatementType::CREATE_STATEMENT) {
        channel.warn(std::format("Unsupported schema statement: {}", magic_enum::enum_name(stmt->type)));
        return false;
    }

    auto& create_stmt = stmt->Cast<duckdb::CreateStatement>();
    
    switch (create_stmt.info->type) {
    case duckdb::CatalogType::TYPE_ENTRY: 
        return true;
    default:
        {
            channel.warn(std::format("Unsupported schema statement: {}/{}", magic_enum::enum_name(stmt->type), magic_enum::enum_name(create_stmt.info->type)));
            return false;
        }
    }
}

static auto executeInternal(duckdb::Connection& conn, duckdb::unique_ptr<duckdb::SQLStatement>& stmt, std::optional<std::string>&& name_alt, NngChannel& channel) -> void {
    if (! isSupportedStatements(stmt, channel)) {
        channel.makeWorkerResponse([](auto& encoder, auto& worker_phase, auto& desc, auto offset) {
            encodeStatementOffset(encoder, worker_phase, desc, offset);
        });
        return;
    }
    
    std::string message;
    try {
        std::optional<UserTypeResult> result;
        try {
            conn.BeginTransaction();
            extract: {
                auto bound_result = bindTypeToStatement(*conn.context, std::move(stmt->Copy()), {}, {});

                result = resolveUserType(bound_result.stmt.plan, channel);
            }
            conn.Commit();
        }
        catch (...) {
            conn.Rollback();
            throw;
        }

        send: {
            if (result) {
                send_user_type: {
                    std::unordered_map<std::string_view, CborEncoder<VectorBackend>> topic_bodies({
                        {topic_user_type, encodeUserType(result.value().entry)},
                        {topic_anon_user_type, encodeAnonymousUserType(std::move(result.value().anon_types))},
                        {topic_bound_user_type, encodeBoundUserType(std::move(result.value().user_type_names))}
                    });

                    channel.makeWorkerResponse([&](auto& encoder, auto& worker_phase, auto& desc, auto offset) {
                        encodeTopicBody(encoder, worker_phase, desc, offset, name_alt, topic_bodies);
                    });
                }
            }
        }

        return;
    }
    catch (const duckdb::Exception& ex) {
        message = ex.what();
    }
    
    channel.err(message);
    channel.makeWorkerResponse([](auto& encoder, auto& worker_phase, auto& desc, auto offset) {
        encodeStatementOffset(encoder, worker_phase, desc, offset);
    });
}

auto UserTypeWorker::messageChannel(const std::optional<size_t>& offset, std::string_view phase) -> NngChannel {
    return NngChannel(this->desc, offset, std::format("{}#{}", this->from_stage, phase));
}

auto UserTypeWorker::rename(std::string&& base_name, const size_t stmt_index, const size_t stmt_count) -> std::optional<std::string> {
    return stmt_count > 1 ? std::make_optional(base_name) : std::nullopt;
}

auto UserTypeWorker::execute(std::string&& query) -> WorkerResultCode {
    auto parse_channel = this->messageChannel(std::nullopt, "parse");
    auto stmts = parseQuery(this->conn, query, parse_channel);
    parse_channel.collectInto(this->results);

    for (size_t stmt_offset = 0; auto& stmt: stmts) {
        auto channel = this->messageChannel(stmt_offset, "user_type");
        executeInternal(
            this->conn, stmt, 
            this->rename(pickUserTypeName(stmt), stmt_offset, stmts.size()),
            channel
        );
        channel.collectInto(this->results);
        ++stmt_offset;
    }

    return no_error;
}

}

extern "C" {
    auto initUserTypeCollector(DatabaseRef db_ref, Slice stage, SourceDescriptor desc, CollectorRef *handle) -> int32_t {
        auto db = reinterpret_cast<worker::Database *>(db_ref);
        auto worker = new worker::UserTypeWorker(db, std::move(stage), std::move(desc));
        *handle = reinterpret_cast<CollectorRef>(worker);
        return 0;
    }

    auto deinitUserTypeCollector(CollectorRef handle) -> void {
        delete reinterpret_cast<worker::UserTypeWorker*>(handle);
    }

    auto describeUserType(CollectorRef handle, Slice query) -> WorkerResultCode {
        auto worker = reinterpret_cast<worker::UserTypeWorker *>(handle);
        return worker->execute(std::string(query.ptr, query.len));
    }

    auto getUserTypeResultCount(CollectorRef handle) -> size_t {
        auto worker = reinterpret_cast<worker::UserTypeWorker *>(handle);
        return worker->results.size();
    }
    
    auto getUserTypeResult(CollectorRef handle, size_t index) -> nng_msg* {
        auto worker = reinterpret_cast<worker::UserTypeWorker *>(handle);
        return worker->results[index];
    }
}
