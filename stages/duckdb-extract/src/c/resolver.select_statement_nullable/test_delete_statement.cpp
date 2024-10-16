#ifndef DISABLE_CATCH2_TEST

#include <catch2/catch_test_macros.hpp>

#include "run.hpp"

using namespace worker;

TEST_CASE("ResolveNullable::Delete statement") {
    SECTION("basic") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            delete from Foo where kind = 4
        )#");

        std::vector<ColumnBindingPair> expects{};
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
    SECTION("has returning/single column") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            delete from Foo where kind = 4
            returning id
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
    SECTION("has returning/single column (nullable)") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            delete from Foo where kind = 4
            returning xys
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
    SECTION("has returning/multi column") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            delete from Foo where kind = 4
            returning kind, xys
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
    SECTION("has returning/tuple") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            delete from Foo where kind = 4
            returning (kind, xys)
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
}

#endif
