#ifndef DISABLE_CATCH2_TEST

#include <catch2/catch_test_macros.hpp>
#include "run.hpp"

using namespace worker;

TEST_CASE("ResolveParam::Update statement") {
    SECTION("whereless") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            update Foo 
            set xys = $v1
        )#");
        ParamNameLookup lookup{
            { "1", ParamLookupEntry("v1") }
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema_1}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("basic") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            update Foo 
            set xys = $v1, remarks = $remarks
            where kind = $kind
        )#");
        ParamNameLookup lookup{
            { "1", ParamLookupEntry("v1") },
            { "2", ParamLookupEntry("remarks") },
            { "3", ParamLookupEntry("kind") }
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "VARCHAR"} }, 
            {"3", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema_1}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("has using reference relation") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            update Foo 
            set xys = b.value, remarks = $remarks
            from Bar b
            where kind = $kind and Foo.id = b.id
        )#");
        ParamNameLookup lookup{
            { "1", ParamLookupEntry("remarks") },
            { "2", ParamLookupEntry("kind") }
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "VARCHAR"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
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
                from Foo
                where kind = $kind
            )
            update Bar 
            set value = $v1
            from X
            where Bar.id = X.id
        )#");
        ParamNameLookup lookup{
            { "1", ParamLookupEntry("kind") },
            { "2", ParamLookupEntry("v1") },
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "VARCHAR"} }, 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, user_type_names, anon_types);
    }
}

#endif