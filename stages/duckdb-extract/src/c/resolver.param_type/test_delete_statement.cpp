#ifndef DISABLE_CATCH2_TEST

#include <catch2/catch_test_macros.hpp>
#include "test/resolver.param_type.hpp"

using namespace worker;

TEST_CASE("Placeholder in delete statement") {
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
    // SECTION("has returning") {
    //     FAIL("Placeholder in delete statement/returning: not implemented");
    // }
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
            )
            delete from Foo 
            using X
            where 
                Foo.id = X.id 
                and Foo.kind = $kind 
                and X.value = $phrase
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

        runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, user_type_names, anon_types);    }
}

#endif
