#ifndef DISABLE_CATCH2_TEST

#include <catch2/catch_test_macros.hpp>
#include "run.hpp"

using namespace worker;

TEST_CASE("ResolveParam::Insert statement") {
    SECTION("basic") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            insert into Foo (id, kind) 
            values ($id, $kind)
        )#");
        ParamNameLookup lookup{
            { "1", ParamLookupEntry("id") },
            { "2", ParamLookupEntry("kind") },
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema_1}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("basic (replace)") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            insert or replace into Foo (id, kind) 
            values ($id, $kind)
        )#");
        ParamNameLookup lookup{
            { "1", ParamLookupEntry("id") },
            { "2", ParamLookupEntry("kind") },
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema_1}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("insert from select") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Foo2 (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            insert into Foo2
            select * replace(xys * $n::int as xys) from Foo
            where 
                kind = $kind 
                and xys is not null
        )#");
        ParamNameLookup lookup{
            { "1", ParamLookupEntry("n") },
            { "2", ParamLookupEntry("kind") },
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("insert from select with CTE") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Foo2 (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            with X as (
                select * from Foo
                where kind = $kind 
            )
            insert into Foo2
            select * replace(xys * $n::int as xys) from X
            where xys is not null
        )#");
        ParamNameLookup lookup{
            { "1", ParamLookupEntry("kind") },
            { "2", ParamLookupEntry("n") },
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("conflict/index condition") {
        std::string schema_1("CREATE TABLE Point (z INTEGER PRIMARY KEY, x INTEGER UNIQUE, y INTEGER)");
        std::string sql(R"#(
            INSERT INTO Point
            VALUES (1, $y, 700)
            ON CONFLICT (z) 
            WHERE z % $div::int = $mod
            DO NOTHING
        )#");
        ParamNameLookup lookup{
            { "1", ParamLookupEntry("y") },
            { "2", ParamLookupEntry("div") },
            { "3", ParamLookupEntry("mod") },
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"3", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema_1}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("conflict/update") {
        std::string schema_1("CREATE TABLE Point (z INTEGER PRIMARY KEY, x INTEGER UNIQUE, y INTEGER)");
        std::string sql(R"#(
            INSERT INTO Point
            VALUES (1, $x, 700)
            ON CONFLICT (z)
            DO UPDATE
            SET y = EXCLUDED.y * $n::int
        )#");
        ParamNameLookup lookup{
            { "1", ParamLookupEntry("x") },
            { "2", ParamLookupEntry("n") },
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema_1}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("conflict/update with where clause") {
        std::string schema_1("CREATE TABLE Point (z INTEGER PRIMARY KEY, x INTEGER UNIQUE, y INTEGER)");
        std::string sql(R"#(
            INSERT INTO Point
            VALUES (1, $x, 700)
            ON CONFLICT (z)
            DO UPDATE
            SET y = EXCLUDED.y * $n::int
            WHERE y is not null and y % $div::int = $mod
        )#");
        ParamNameLookup lookup{
            { "1", ParamLookupEntry("x") },
            { "2", ParamLookupEntry("n") },
            { "3", ParamLookupEntry("div") },
            { "4", ParamLookupEntry("mod") },
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"3", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"4", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema_1}, lookup, bound_types, user_type_names, anon_types);
    }
}

TEST_CASE("TransformeSQL: Insert statement") {
    SECTION("basic") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            insert into Foo (id, kind) 
            values ($id, $kind)
        )#");
        std::string expect_sql(R"#(INSERT INTO Foo (id, kind ) (VALUES ($1, $2)))#");

        runTransformQuery(sql, {schema_1}, expect_sql);
    }
    SECTION("insert with returning") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            insert into Foo (id, kind) 
            values ($id, $kind)
            returning id
        )#");
        std::string expect_sql(R"#(INSERT INTO Foo (id, kind ) (VALUES ($1, $2)) RETURNING id)#");

        runTransformQuery(sql, {schema_1}, expect_sql);
    }
    SECTION("insert with returning") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            insert into Foo (id, kind) 
            values ($id, $kind)
            returning id as inserted
        )#");
        std::string expect_sql(R"#(INSERT INTO Foo (id, kind ) (VALUES ($1, $2)) RETURNING id AS inserted)#");

        runTransformQuery(sql, {schema_1}, expect_sql);
    }
    SECTION("insert with returning tuple w/o alias") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            insert into Foo (id, kind) 
            values ($id, $kind)
            returning (kind, id)
        )#");
        std::string expect_sql(R"#(INSERT INTO Foo (id, kind ) (VALUES ($1, $2)) RETURNING main."row"(kind, id) AS "main.""row""(kind, id)")#");

        runTransformQuery(sql, {schema_1}, expect_sql);
    }
    SECTION("insert with returning tuple with alias") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            insert into Foo (id, kind) 
            values ($id, $kind)
            returning (kind, id) as inserted
        )#");
        std::string expect_sql(R"#(INSERT INTO Foo (id, kind ) (VALUES ($1, $2)) RETURNING main."row"(kind, id) AS inserted)#");

        runTransformQuery(sql, {schema_1}, expect_sql);
    }
}
#endif
