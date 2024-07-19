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
private:
    duckdb::DuckDB db;
};

}
