#include <duckdb.hpp>
#include "zmq_worker_support.hpp"

namespace worker {

auto walkSelectStatement(duckdb::SelectStatement& stmt, ZmqChannel zmq_channel) -> void;
auto bindTableRef(duckdb::ClientContext context, duckdb::SQLStatement& stmt) -> duckdb::unique_ptr<duckdb::BoundTableRef>;

}