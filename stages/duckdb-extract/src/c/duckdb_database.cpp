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

static auto initSchemaInternal(duckdb::Connection& conn, const fs::path& schema_file_path) -> WorkerResultCode {
    if (! extensionAccepted(schema_file_path.extension())) return no_error;

    auto file = std::ifstream(schema_file_path);
    auto content = std::string(
        std::istreambuf_iterator<char>(file),
        std::istreambuf_iterator<char>()
    );

    try {
        conn.Query(content);
        return no_error;
    }
    catch (const duckdb::Exception& ex) {
        return schema_load_failed;
    }
}

// --------------------------------------------------------------------------------------------------------------

Database::Database(): db(duckdb::DuckDB(nullptr)) {}

auto Database::connect() -> duckdb::Connection {
    return duckdb::Connection(this->db);
}

auto Database::loadSchemaAll(const fs::path& schema_dir) -> WorkerResultCode {
    std::error_code err;
    if (! fs::exists(schema_dir, err)) {
        return schema_dir_not_found;
    }

    auto conn = this->connect();
    auto dir = fs::recursive_directory_iterator(schema_dir);

    for(auto& entry: dir) {
        if (entry.is_regular_file()) {
            auto err = initSchemaInternal(conn, entry.path());
            if (err != no_error) return err;
        }
    }

    return no_error;
}

}

// --------------------------------------------------------------------------------------------------------------

extern "C" {
    auto initDatabase(DatabaseRef *handle) -> int32_t {
        *handle = nullptr;

        auto db = new worker::Database();
        *handle = reinterpret_cast<DatabaseRef>(db);

        return 0;
    }

    auto deinitDatabase(DatabaseRef handle) -> void {
        delete reinterpret_cast<worker::Database *>(handle);
    }

    auto loadSchema(DatabaseRef handle, const char *schema_dir_path, size_t schema_dir_len) -> int32_t {
        auto db = reinterpret_cast<worker::Database *>(handle);
        return db->loadSchemaAll(std::string(schema_dir_path, schema_dir_len));
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