#include <duckdb.hpp>
#include "zmq_worker_support.hpp"

auto sendWorkerLog(void *socket, std::string from, std::string log_level, std::string message) -> void;
auto sendWorkerResult(void *socket, std::string from, std::vector<char> payload) -> void;

auto walkSelectStatement(duckdb::SelectStatement& stmt, ZmqChannel zmq_channel) -> void;
auto bindTableRef(duckdb::ClientContext context, duckdb::SQLStatement& stmt) -> duckdb::unique_ptr<duckdb::BoundTableRef>;