#ifndef DISABLE_CATCH2_TEST

#include <catch2/catch_test_macros.hpp>
#include "run.hpp"

using namespace worker;

TEST_CASE("ResolveParam::Delete statement") {
    SECTION("basic") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("delete from Foo where kind = $kind");
        ParamNameLookup lookup{
            { "1", ParamLookupEntry("kind") }
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema_1}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("has using reference relation") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            delete from Foo 
            using Bar
            where 
                Foo.id = Bar.id 
                and Foo.kind = $kind 
                and Bar.value = $phrase
        )#");
        ParamNameLookup lookup{
            { "1", ParamLookupEntry("kind") },
            { "2", ParamLookupEntry("phrase") }
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "VARCHAR"} }, 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("has CTE") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            with X as (
                from bar
                where value = $value
            )
            delete from Foo 
            using X
            where 
                Foo.id = X.id 
                and Foo.kind = $kind 
                and X.value = $phrase
        )#");
        ParamNameLookup lookup{
            { "1", ParamLookupEntry("value") },
            { "2", ParamLookupEntry("kind") },
            { "3", ParamLookupEntry("phrase") }
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "VARCHAR"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"3", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "VARCHAR"} }, 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, user_type_names, anon_types);
    }
}

TEST_CASE("TransformeSQL: Delete statement") {
    SECTION("delete with returning") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("delete from Foo where kind = $kind returning id");
        std::string expect_sql(R"#(DELETE FROM Foo WHERE (kind = $1) RETURNING id)#");

        ParamNameLookup lookup{
            { "1", ParamLookupEntry("kind") }
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
        };
        runTransformQuery(sql, {schema_1}, expect_sql);
    }
    SECTION("delete with returning with alias") {
        SKIP("alias of returning clause is not supported");
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("delete from Foo where kind = $kind returning id as deleted");
        std::string expect_sql(R"#(DELETE FROM Foo WHERE (kind = $1) RETURNING id AS deleted)#");

        ParamNameLookup lookup{
            { "1", ParamLookupEntry("kind") }
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
        };
        runTransformQuery(sql, {schema_1}, expect_sql);
    }
    SECTION("delete with returning tuple w/o alias") {
        SKIP("alias of returning clause is not supported");
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("delete from Foo where kind = $kind returning (kind, id)");
        std::string expect_sql(R"#(DELETE FROM Foo WHERE (kind = $1) RETURNING (kind, id) AS "main.""row""(kind, id)")#");

        ParamNameLookup lookup{
            { "1", ParamLookupEntry("kind") }
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
        };
        runTransformQuery(sql, {schema_1}, expect_sql);
    }
    SECTION("delete with returning tuple with alias") {
        SKIP("alias of returning clause is not supported");
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("delete from Foo where kind = $kind returning (kind, id) as deleted");
        std::string expect_sql(R"#(DELETE FROM Foo WHERE (kind = $1) RETURNING (kind, id) AS deleted)#");

        ParamNameLookup lookup{
            { "1", ParamLookupEntry("kind") }
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
        };
        runTransformQuery(sql, {schema_1}, expect_sql);
    }
}

#endif
