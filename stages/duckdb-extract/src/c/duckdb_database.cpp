#include <fstream>
#include <algorithm>
#include <cctype>

#include <duckdb.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
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

using Dep = std::string;
using PendingQueries = std::vector<std::string>;
using PendingMap = std::unordered_map<Dep, PendingQueries>;

static auto initSchemaInternal(duckdb::Connection& conn, const std::string& schema_sql, PendingMap& pending_map) -> void {
    auto pending = conn.PendingQuery(schema_sql);
    if (pending->HasError()) {
        auto& err_ext = pending->GetErrorObject().ExtraInfo();
        std::string key("name");
        if (err_ext.contains(key)) {
            pending_map[err_ext.at(key)].push_back(std::move(schema_sql));
        }
        return;
    }

    pending->Execute();
}

static auto containsDepencendy(duckdb::Connection& conn, std::string rel_name) -> bool {
    type_catalog: {
        auto catalog = duckdb::Catalog::GetEntry<duckdb::TypeCatalogEntry>(*conn.context, "", "", rel_name, duckdb::OnEntryNotFound::RETURN_NULL);
        if (catalog) return true;
    }
    table_catalog: {
        auto catalog = duckdb::Catalog::GetEntry<duckdb::TableCatalogEntry>(*conn.context, "", "", rel_name, duckdb::OnEntryNotFound::RETURN_NULL);
        if (catalog) return true;
    }
    return false;
}

static auto retryLoadSchema(duckdb::Connection& conn, PendingMap&& retry_map) -> void {
    std::queue<std::string> queue;
    size_t left_count = 0;

    // initialize queue
    for (const auto& [key, queries]: retry_map) {
        queue.push(key);
        // accumulate total count
        left_count += queries.size();
    }

    while (true) {
        std::queue<std::string> next_queue;
        size_t unprocessed_count = 0;

        while (!queue.empty()) {
            std::string current = queue.front();
            queue.pop();

            if (containsDepencendy(conn, current)) {
                std::vector<std::string> next_pendings;

                for (const auto& sql : retry_map[current]) {
                    auto pending_result = conn.PendingQuery(sql);
                    if (pending_result->HasError()) {
                        next_pendings.push_back(sql);
                    }

                    pending_result->Execute();
                }

                if (next_pendings.empty()) {
                    // Successfull for queries of dependency key
                    retry_map.erase(current);
                    continue;
                }
                else {
                    // partially successed
                    retry_map[current] = std::move(next_pendings);
                }
            }

            unprocessed_count += retry_map[current].size();
            next_queue.push(current);
        }

        if (left_count == unprocessed_count) {
            // avoid inifinity loop
            break;
        }
        
        queue = std::move(next_queue);
        left_count = unprocessed_count;
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
    PendingMap retry_map{};
    WorkerResultCode result = no_error;

    auto conn = this->connect();
    conn.BeginTransaction();

    try {
        if (fs::is_regular_file(schema_dir)) {
            auto file = std::ifstream(schema_dir);
            auto content = std::string(
                std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()
            );
            initSchemaInternal(conn, content, retry_map);            
        }
        else {
            auto dir = fs::recursive_directory_iterator(schema_dir);

            for(auto& entry: dir) {
                if (entry.is_regular_file()) {
                    if (! extensionAccepted(entry.path().extension())) continue;

                    auto file = std::ifstream(entry.path());
                    auto content = std::string(
                        std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()
                    );
                    initSchemaInternal(conn, content, retry_map);
                }
            }
        }
        if (retry_map.size() > 0) {
            retryLoadSchema(conn, std::move(retry_map));
        }
    }
    catch (const duckdb::Exception& ex) {
        result = invalid_sql;
    }
    conn.Commit();

    return result;
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

#include <duckdb/common/constants.hpp>
#include <iostream>

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

TEST_CASE("Load schema file#1 (single query)") {
    worker::Database db;
    auto err = db.loadSchemaAll(std::string("./_schema-examples/tables/Foo.sql"));

    load_result_code: {
        UNSCOPED_INFO("load result code");
        CHECK(err == 0);
    }
    reated_schema_information: {
        INFO("created schema information");
        auto conn = db.connect();

        rel_1: {
            UNSCOPED_INFO("relation#1");
            auto info = conn.TableInfo("Foo");
            REQUIRE((bool)info == true);
            REQUIRE(info->columns.size() == 4);
        }
    }
}

TEST_CASE("Load schemas#2 (dependent inconsistency of user type)") {
    std::string schema_1(R"#(CREATE TABLE V (vis B not null))#");
    std::string schema_2(R"#(CREATE TYPE B AS ENUM ('hide', 'visible'))#");
    std::string schema_3(R"#(
        CREATE TABLE T2 (
            id INTEGER PRIMARY KEY,
            t3_id INTEGER,
            t1_id INTEGER,
            FOREIGN KEY (t3_id) REFERENCES T3 (id),
            FOREIGN KEY (t1_id) REFERENCES T1 (id)
        );
    )#");

    worker::Database db;
    auto conn = db.connect();

    worker::PendingMap retry_map{};

    conn.BeginTransaction();

    missing_query: {
        worker::initSchemaInternal(conn, schema_1, retry_map);

        UNSCOPED_INFO("Missing relation query");
        REQUIRE(retry_map.size() == 1);
        REQUIRE(retry_map.contains("B"));
        REQUIRE(retry_map["B"].size() == 1);
        REQUIRE_THAT(retry_map["B"][0], Equals(schema_1));
    }
    valid_query: {
        worker::initSchemaInternal(conn, schema_2, retry_map);

        UNSCOPED_INFO("Valid query");
        REQUIRE(retry_map.size() == 1);
        REQUIRE(retry_map.contains("B"));
        REQUIRE(retry_map["B"].size() == 1);
        REQUIRE_THAT(retry_map["B"][0], Equals(schema_1));
    }
    retry_query: {
        worker::retryLoadSchema(conn, std::move(retry_map));

        UNSCOPED_INFO("Retry query");
        REQUIRE(worker::containsDepencendy(conn, "B"));
    }
    conn.Commit();
}

TEST_CASE("Load schemas#2 (dependent inconsistency of table)") {
    std::string schema_1(R"#(
        CREATE TABLE T2 (
            id INTEGER PRIMARY KEY,
            t1_id INTEGER,
            FOREIGN KEY (t1_id) REFERENCES T1 (id)
        );
    )#");
    std::string schema_2("CREATE TABLE t1 (id INTEGER PRIMARY KEY, j VARCHAR)");

    worker::Database db;
    auto conn = db.connect();

    worker::PendingMap retry_map{};

    conn.BeginTransaction();

    missing_query: {
        worker::initSchemaInternal(conn, schema_1, retry_map);

        UNSCOPED_INFO("Missing relation query");
        REQUIRE(retry_map.size() == 1);
        REQUIRE(retry_map.contains("T1"));
        REQUIRE(retry_map["T1"].size() == 1);
        REQUIRE_THAT(retry_map["T1"][0], Equals(schema_1));
    }
    valid_query: {
        worker::initSchemaInternal(conn, schema_2, retry_map);

        UNSCOPED_INFO("Valid query");
        REQUIRE(retry_map.size() == 1);
        REQUIRE(retry_map.contains("T1"));
        REQUIRE(retry_map["T1"].size() == 1);
        REQUIRE_THAT(retry_map["T1"][0], Equals(schema_1));
    }
    retry_query: {
        worker::retryLoadSchema(conn, std::move(retry_map));

        UNSCOPED_INFO("Retry query");
        REQUIRE(worker::containsDepencendy(conn, "T1"));
    }
    conn.Commit();
}

#endif