#ifndef DISABLE_CATCH2_TEST

#include <catch2/catch_test_macros.hpp>

#include "run.hpp"

using namespace worker;

TEST_CASE("ResolveNullable::Insert statement") {
    SECTION("basic") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            insert into Foo (id, kind) 
            values ($id, $kind)
        )#");

        std::vector<ColumnBindingPair> expects{};
    
        runResolveSelectListNullability(sql, {schema_1}, expects);
    }
    SECTION("has returning/single column") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            insert into Foo (id, kind) 
            values ($id, $kind)
            returning id
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(10, 0), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema_1}, expects);
    }
    SECTION("has returning/single column (nullable)") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            insert into Foo (id, kind) 
            values ($id, $kind)
            returning xys
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(10, 0), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema_1}, expects);
    }
    SECTION("has returning/multi column") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            insert into Foo (id, kind) 
            values ($id, $kind)
            returning xys, id
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(10, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(10, 1), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema_1}, expects);
    }
    SECTION("has returning/tuple") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            insert into Foo (id, kind) 
            values ($id, $kind)
            returning (xys, id)
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(10, 0), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema_1}, expects);
    }
}

#endif
