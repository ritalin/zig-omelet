#pragma once

#include <filesystem>

#include <duckdb.hpp>
#include "duckdb_worker.h"

namespace worker {

class Database {
public:
    Database();
public:
    auto connect() -> duckdb::Connection;
    auto loadSchemaAll(const std::filesystem::path& schema_dir) -> WorkerResultCode;
    auto retainUserTypeName(duckdb::Connection& conn) -> void;
private:
    duckdb::DuckDB db;
};

}
