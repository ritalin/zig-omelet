#include <duckdb.hpp>
#include "zmq_worker_support.hpp"
#include "duckdb_params_collector.hpp"

namespace worker {

auto evalParameterType(const duckdb::unique_ptr<duckdb::SQLStatement>& stmt) -> ParameterCollector::ParameterType;
auto bindTableRef(duckdb::ClientContext context, duckdb::SQLStatement& stmt) -> duckdb::unique_ptr<duckdb::BoundTableRef>;

}