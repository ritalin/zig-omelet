#include <duckdb.hpp>
#include <fstream>

#include "duckdb_database.hpp"
#include "duckdb_worker.h"

namespace worker {

static auto initSchemaInternal(duckdb::Connection& conn, const std::string& schema_file_path) -> void {
    auto file = std::ifstream(schema_file_path);
    auto content = std::string(
        std::istreambuf_iterator<char>(file),
        std::istreambuf_iterator<char>()
    );

    conn.Query(content);
}

// --------------------------------------------------------------------------------------------------------------

Database::Database(): db(duckdb::DuckDB(nullptr)) {}

auto Database::connect() -> duckdb::Connection {
    return duckdb::Connection(this->db);
}

namespace fs = std::filesystem;

auto Database::loadSchemaAll(const fs::path& schema_dir) -> WorkerResultCode {
    std::error_code err;
    if (! fs::exists(schema_dir, err)) {
        return schema_file_not_found;
    }

    auto conn = this->connect();
    auto dir = fs::recursive_directory_iterator(schema_dir);

    for(auto& entry: dir) {
        if (entry.is_regular_file()) {
            initSchemaInternal(conn, entry.path());
        }
    }

    return no_error;
}

}

// --------------------------------------------------------------------------------------------------------------

extern "C" {
    auto initDatabase(const char *schema_dir_path, size_t schema_dir_len, DatabaseRef *handle) -> int32_t {
        auto db = new worker::Database();
        auto result = db->loadSchemaAll(std::string(schema_dir_path, schema_dir_len));

        if (result == 0) {
            *handle = reinterpret_cast<DatabaseRef>(db);
        }

        *handle = nullptr;
        return result;
    }

    auto deinitDatabase(DatabaseRef handle) -> void {
        delete reinterpret_cast<worker::Database *>(handle);
    }
}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

using namespace Catch::Matchers;

TEST_CASE("Schema not found...") {
    worker::Database db;
    auto err = db.loadSchemaAll(std::string("path/to"));
    SECTION("load result code") {
        CHECK(err != 0);
    }
}

TEST_CASE("Load schemas") {
    worker::Database db;
    auto err = db.loadSchemaAll(std::string("./_schema-examples"));
    SECTION("load result code") {
        CHECK(err == 0);
    }
    SECTION("created schema information") {
        auto conn = db.connect();

        SECTION("relation#1") {
            auto info = conn.TableInfo("Foo");
            REQUIRE(info);
            REQUIRE(info->columns.size() == 2);

            SECTION("find column#1") {
                auto result = std::find_if(info->columns.begin(), info->columns.end(), [](duckdb::ColumnDefinition& c) {
                    return c.GetName() == "kind";
                });
                CHECK(result != info->columns.end());

                CHECK_THAT(result->GetName(), Equals("kind"));
                CHECK_THAT(result->GetType().ToString(), Equals("INTEGER"));
            }
            SECTION("find column#2") {
                auto result = std::find_if(info->columns.begin(), info->columns.end(), [](duckdb::ColumnDefinition& c) {
                    return c.GetName() == "xyz";
                });
                CHECK(result != info->columns.end());

                CHECK_THAT(result->GetName(), Equals("xyz"));
                CHECK_THAT(result->GetType().ToString(), Equals("VARCHAR"));
            }
        }
        SECTION("relation#2") {
            auto info = conn.TableInfo("Point");
            REQUIRE(info);
            REQUIRE(info->columns.size() == 3);

            SECTION("find column#1") {
                auto result = std::find_if(info->columns.begin(), info->columns.end(), [](duckdb::ColumnDefinition& c) {
                    return c.GetName() == "x";
                });
                CHECK(result != info->columns.end());

                CHECK_THAT(result->GetName(), Equals("x"));
                CHECK_THAT(result->GetType().ToString(), Equals("INTEGER"));
            }
            SECTION("find column#2") {
                auto result = std::find_if(info->columns.begin(), info->columns.end(), [](duckdb::ColumnDefinition& c) {
                    return c.GetName() == "y";
                });
                CHECK(result != info->columns.end());

                CHECK_THAT(result->GetName(), Equals("y"));
                CHECK_THAT(result->GetType().ToString(), Equals("INTEGER"));
            }
            SECTION("find column#3") {
                auto result = std::find_if(info->columns.begin(), info->columns.end(), [](duckdb::ColumnDefinition& c) {
                    return c.GetName() == "z";
                });
                CHECK(result != info->columns.end());

                CHECK_THAT(result->GetName(), Equals("z"));
                CHECK_THAT(result->GetType().ToString(), Equals("INTEGER"));
            }
        }
    }
}

#endif