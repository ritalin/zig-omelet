#include <duckdb.hpp>

#include "duckdb_worker.h"
#include "duckdb_database.hpp"
#include "zmq_worker_support.hpp"
#include "cbor_encode.hpp"
#include "duckdb_binder_support.hpp"

namespace worker {

class UserTypeWorker {
public:
    UserTypeWorker(worker::Database *db, std::string&& id, std::optional<void*>&& socket): conn(db->connect()), id(id), socket(socket) {}
public:
    auto execute(std::string&& query) -> WorkerResultCode;
    auto messageChannel(const std::string& from) -> ZmqChannel;
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

static auto executeInternal(duckdb::Connection& conn, duckdb::unique_ptr<duckdb::SQLStatement>& stmt, int32_t stmt_offset, int32_t stmt_size, ZmqChannel&& channel) -> void {
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

                channel.sendWorkerResult(stmt_offset, stmt_size, topic_bodies);
            }
        }
    }
}

auto UserTypeWorker::messageChannel(const std::string& from) -> ZmqChannel {
    return ZmqChannel(this->socket, this->id, from);
}

auto UserTypeWorker::execute(std::string&& query) -> WorkerResultCode {
    std::string message;
    try {
        auto stmts = this->conn.ExtractStatements(query);

        if (stmts.size() > 0) {
            const int32_t stmt_offset = 1;
            const int32_t stmt_size = 1;

            executeInternal(this->conn, stmts[0], stmt_offset, stmt_size, this->messageChannel("worker.user_type"));
        }

        return no_error;
    }
    catch (const duckdb::Exception& ex) {
        message = ex.what();
    }
    
    this->messageChannel("worker").err(message);
    return invalid_sql;
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
