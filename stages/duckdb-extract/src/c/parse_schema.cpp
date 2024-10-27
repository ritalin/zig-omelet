#include <duckdb.hpp>
#include <duckdb/parser/statement/create_statement.hpp>

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
    UserTypeWorker(worker::Database *db, std::string&& id, std::optional<void*>&& socket): conn(db->connect()), id(id), socket(socket) {}
public:
    auto execute(std::string&& query) -> WorkerResultCode;
    auto messageChannel(const std::optional<size_t>& offset, const std::string& from) -> ZmqChannel;
private:
    duckdb::Connection conn;
    std::string id;
    std::optional<void*> socket;
};

static auto encodeUserType(const UserTypeEntry& entry) -> std::vector<char> {
    CborEncoder encoder;
    encodeUserType(encoder, entry);

    return std::move(encoder.rawBuffer());
}

static auto encodeBoundUserType(std::vector<std::string>&& user_types) -> std::vector<char> {
    CborEncoder encoder;

    encoder.addArrayHeader(user_types.size());

    for (auto& name: user_types) {
        encoder.addString(name);
    }

    return std::move(encoder.rawBuffer());
}

static auto encodeAnonymousUserType(std::vector<UserTypeEntry>&& anon_types) -> std::vector<char> {
    CborEncoder encoder;

    encoder.addArrayHeader(anon_types.size());

    for (auto& entry: anon_types) {
        encodeUserType(encoder, entry);
    }

    return std::move(encoder.rawBuffer());
}

static auto parseQuery(duckdb::Connection& conn, std::string query, ZmqChannel&& channel) -> std::vector<duckdb::unique_ptr<duckdb::SQLStatement>> {
    if (query == "") {
        channel.warn("Cannot handle an empty schema");
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
    channel.sendWorkerResponse(::worker_skipped, encodeStatementCount(0));

    return {};
}

static auto isSupportedStatements(duckdb::unique_ptr<duckdb::SQLStatement>& stmt, ZmqChannel& channel) -> bool {
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

static auto executeInternal(duckdb::Connection& conn, const size_t stmt_offset, duckdb::unique_ptr<duckdb::SQLStatement>& stmt, ZmqChannel&& channel) -> void {
    if (! isSupportedStatements(stmt, channel)) {
        channel.sendWorkerResponse(::worker_skipped, encodeStatementOffset(stmt_offset));
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
                    std::unordered_map<std::string, std::vector<char>> topic_bodies({
                        {topic_user_type, encodeUserType(result.value().entry)},
                        {topic_anon_user_type, encodeAnonymousUserType(std::move(result.value().anon_types))},
                        {topic_bound_user_type, encodeBoundUserType(std::move(result.value().user_type_names))}
                    });

                    channel.sendWorkerResponse(::worker_result, encodeTopicBody(stmt_offset, topic_bodies));
                }
            }
        }

        return;
    }
    catch (const duckdb::Exception& ex) {
        message = ex.what();
    }
    
    channel.err(message);
}

auto UserTypeWorker::messageChannel(const std::optional<size_t>& offset, const std::string& from) -> ZmqChannel {
    return ZmqChannel(this->socket, offset, this->id, from);
}

auto UserTypeWorker::execute(std::string&& query) -> WorkerResultCode {
    auto stmts = parseQuery(this->conn, query, this->messageChannel(std::nullopt, "worker.phase.parse"));

    const int32_t stmt_offset = 0;
    if (stmts.size() > 0) {
        executeInternal(this->conn, stmt_offset, stmts[stmt_offset], this->messageChannel(stmt_offset, "worker.phase.user_type"));
    }

    this->messageChannel(std::nullopt, "worker.phase.done").sendWorkerResponse(::worker_finished, {});

    return no_error;
}

}

extern "C" {
    auto initUserTypeCollector(DatabaseRef db_ref, const char *id, size_t id_len, void *socket, CollectorRef *handle) -> int32_t {
        auto db = reinterpret_cast<worker::Database *>(db_ref);
        auto worker = new worker::UserTypeWorker(db, std::string(id, id_len), socket ? std::make_optional(socket) : std::nullopt);
        *handle = reinterpret_cast<CollectorRef>(worker);
        return 0;
    }

    auto deinitUserTypeCollector(CollectorRef handle) -> void {
        delete reinterpret_cast<worker::UserTypeWorker*>(handle);
    }

    auto describeUserType(CollectorRef handle, const char *query, size_t query_len) -> WorkerResultCode {
        auto worker = reinterpret_cast<worker::UserTypeWorker *>(handle);
        return worker->execute(std::string(query, query_len));
    }
}
