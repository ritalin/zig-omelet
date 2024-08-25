#include <fstream>
#include <algorithm>
#include <cctype>

#include <duckdb.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/common/extra_type_info.hpp>

#include "duckdb_database.hpp"
#include "duckdb_binder_support.hpp"
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

auto Database::retainUserTypeName(duckdb::Connection& conn) -> void {
    conn.BeginTransaction();
    try {
        auto schemas = duckdb::Catalog::GetAllSchemas(*conn.context);
        for (auto &schema: schemas) {
            schema.get().Scan(*conn.context, duckdb::CatalogType::TYPE_ENTRY, [&](duckdb::CatalogEntry &entry) { 
                if (! entry.internal) {
                    auto& type_entry = entry.Cast<duckdb::TypeCatalogEntry>();
                    type_entry.user_type.SetAlias(type_entry.name);
                }
            });
        };
        conn.Commit();
    }
    catch(...) {
        conn.Rollback();
        throw;
    }

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

    auto loadSchema(DatabaseRef handle, const char *schema_dir_path, size_t schema_dir_len) -> WorkerResultCode {
        auto db = reinterpret_cast<worker::Database *>(handle);
        return db->loadSchemaAll(std::string(schema_dir_path, schema_dir_len));
    }

    auto retainUserTypeName(DatabaseRef handle) -> WorkerResultCode {
        auto db = reinterpret_cast<worker::Database *>(handle);
        try {
            auto conn = db->connect();
            db->retainUserTypeName(conn);
            return no_error;
        }
        catch (...) {
            return invalid_schema_catalog;
        }
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

    load_result_code: {
        UNSCOPED_INFO("load result code");
        CHECK(err == 0);
    }
    created_schema_information: {
        INFO("created schema information");
        auto conn = db.connect();

        rel_1: {
            UNSCOPED_INFO("relation#1");
            auto info = conn.TableInfo("Foo");
            REQUIRE((bool)info == true);
            REQUIRE(info->columns.size() == 4);

            rel_1_col_1: {
                UNSCOPED_INFO("column#1");
                auto result = std::find_if(info->columns.begin(), info->columns.end(), [](duckdb::ColumnDefinition& c) {
                    return c.GetName() == "id";
                });
                CHECK(result != info->columns.end());

                CHECK_THAT(result->GetName(), Equals("id"));
                CHECK_THAT(result->GetType().ToString(), Equals("INTEGER"));
            }
            rel_1_col_2: {
                UNSCOPED_INFO("column#2");
                auto result = std::find_if(info->columns.begin(), info->columns.end(), [](duckdb::ColumnDefinition& c) {
                    return c.GetName() == "kind";
                });
                CHECK(result != info->columns.end());

                CHECK_THAT(result->GetName(), Equals("kind"));
                CHECK_THAT(result->GetType().ToString(), Equals("INTEGER"));
            }
            rel_1_col_3: {
                UNSCOPED_INFO("column#3");
                auto result = std::find_if(info->columns.begin(), info->columns.end(), [](duckdb::ColumnDefinition& c) {
                    return c.GetName() == "xyz";
                });
                CHECK(result != info->columns.end());

                CHECK_THAT(result->GetName(), Equals("xyz"));
                CHECK_THAT(result->GetType().ToString(), Equals("INTEGER"));
            }
            col_4: {
                UNSCOPED_INFO("column#4");
                auto result = std::find_if(info->columns.begin(), info->columns.end(), [](duckdb::ColumnDefinition& c) {
                    return c.GetName() == "remarks";
                });
                CHECK(result != info->columns.end());

                CHECK_THAT(result->GetName(), Equals("remarks"));
                CHECK_THAT(result->GetType().ToString(), Equals("VARCHAR"));
            }
        }
        rel_2: {
            UNSCOPED_INFO("relation#2");
            auto info = conn.TableInfo("Point");
            REQUIRE(info);
            REQUIRE(info->columns.size() == 3);

            rel_2_col_1: {
                UNSCOPED_INFO("find column#1");
                auto result = std::find_if(info->columns.begin(), info->columns.end(), [](duckdb::ColumnDefinition& c) {
                    return c.GetName() == "x";
                });
                CHECK(result != info->columns.end());

                CHECK_THAT(result->GetName(), Equals("x"));
                CHECK_THAT(result->GetType().ToString(), Equals("INTEGER"));
            }
            rel_2_col_2: {
                UNSCOPED_INFO("find column#2");
                auto result = std::find_if(info->columns.begin(), info->columns.end(), [](duckdb::ColumnDefinition& c) {
                    return c.GetName() == "y";
                });
                CHECK(result != info->columns.end());

                CHECK_THAT(result->GetName(), Equals("y"));
                CHECK_THAT(result->GetType().ToString(), Equals("INTEGER"));
            }
            rel_2_col_3: {
                UNSCOPED_INFO("find column#3");
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