#include <fstream>
#include <algorithm>
#include <cctype>

#include <duckdb.hpp>

#include "duckdb_database.hpp"
#include "duckdb_worker.h"

namespace worker {

namespace fs = std::filesystem;

static auto extensionAccepted(const std:: string& ext) -> bool {
    std::string canonical_ext = ext;
    std::transform(
        ext.begin(), ext.end(), canonical_ext.begin(),
        [](unsigned char c){ return std::tolower(c); }
    );

    return canonical_ext == ".sql";
}

static auto initSchemaInternal(duckdb::Connection& conn, const fs::path& schema_file_path) -> void {
    if (! extensionAccepted(schema_file_path.extension())) return;

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
        *handle = nullptr;

        auto db = new worker::Database();
        auto result = db->loadSchemaAll(std::string(schema_dir_path, schema_dir_len));

        if (result == 0) {
            *handle = reinterpret_cast<DatabaseRef>(db);
        }

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
            REQUIRE((bool)info == true);
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