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

static auto userTypeKindAsText(UserTypeKind kind) -> std::string {
    switch (kind) {
    case UserTypeKind::Enum: return std::move(std::to_string('enum'));
    }
}

static auto encodeUserType(const UserTypeEntry& entry) -> std::vector<char> {
    CborEncoder encoder;

    encoder.addArrayHeader(2);
    type_header: {
        type_kind: {
            encoder.addString(userTypeKindAsText(entry.kind));
        }
        type_name: {
            encoder.addString(entry.name);
        }
    }
    type_bodies: {
        encoder.addArrayHeader(fields.size());
        for (auto& field: fields) {
            encoder.addArrayHeader(2);
            encoder.addString(field.field_name);
            if (field.field_type) {
                encoder.addString(field.field_type.value());
            }
            else {
                encoder.addNull();
            }
        }
    }

    return std::move(encoder.rawBuffer());
}

static auto executeInternal(duckdb::Connection& conn, duckdb::unique_ptr<duckdb::SQLStatement>& stmt, int32_t stmt_offset, int32_t stmt_size, ZmqChannel&& channel) -> void {
    std::optional<UserTypeEntry> entry;
    try {
        conn.BeginTransaction();
        extract: {
            auto bound_stmt = bindTypeToStatement(*conn.context, std::move(stmt->Copy()));

            entry = resolveUserType(bound_stmt.plan, channel);
        }
        conn.Commit();
    }
    catch (...) {
        conn.Rollback();
        throw;
    }

    send: {
        if (entry) {
            std::unordered_map<std::string, std::vector<char>> topic_bodies({
                {topic_user_type, encodeUserType(entry.value())},
            });

            channel.sendWorkerResult(stmt_offset, stmt_size, topic_bodies);
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
