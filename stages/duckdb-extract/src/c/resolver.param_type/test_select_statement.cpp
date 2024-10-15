#ifndef DISABLE_CATCH2_TEST

#include <catch2/catch_test_macros.hpp>
#include "test/resolver.param_type.hpp"

using namespace worker;

TEST_CASE("ResolveParam::without parameters") {
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string sql("select * from Foo");
    ParamNameLookup lookup{};
    ExpectParamLookup bound_types{};
    UserTypeExpects user_type_names{};
    AnonTypeExpects anon_types{};

    runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
}

TEST_CASE("ResolveParam::positional parameter") {
    SECTION("where clause") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select * from Foo where kind = $1");
        ParamNameLookup lookup{
            { "1", ParamLookupEntry("1") }
        };
        ExpectParamLookup bound_types{{"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} } };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("select list and where clause") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select id, $2::text from Foo where kind = $1");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("1")}, 
            {"2", ParamLookupEntry("2")}
        };
        ExpectParamLookup bound_types{ 
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "VARCHAR"} } 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("filter clause") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select sum($val::int) filter (fmod(id, $div::int) > $rem::int) as a from Foo");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("val")}, 
            {"2", ParamLookupEntry("div")}, 
            {"3", ParamLookupEntry("rem")}
        };
        ExpectParamLookup bound_types{ 
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"3", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} } 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
}

TEST_CASE("ResolveParam::named parameter") {
    SECTION("where clause") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select * from Foo where kind = $kind");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("kind")}
        };
        ExpectParamLookup bound_types{{"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }};
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("select list and where clause") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select id, $phrase::text from Foo where kind = $kind");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("phrase")}, 
            {"2", ParamLookupEntry("kind")}
            };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "VARCHAR"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("joined select list and where clause") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select Foo.id, $phrase::text 
            from Foo 
            join Bar on Foo.id = Bar.id and Bar.value <> $serch_word
            where Foo.kind = $kind
        )#");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("phrase")}, 
            {"2", ParamLookupEntry("serch_word")}, 
            {"3", ParamLookupEntry("kind")}};
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "VARCHAR"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "VARCHAR"} }, 
            {"3", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("select list and where clause with subquery") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            select x.*, null, '' as "''", 1+2, $seq::int as n
            from (
                select id, kind, 123::bigint, $phrase::text as s
                from Foo
                where kind = $kind
            ) x
        )#");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("seq")}, 
            {"2", ParamLookupEntry("phrase")}, 
            {"3", ParamLookupEntry("kind")}
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "VARCHAR"} }, 
            {"3", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("duplicated named parameter#1 (same type)") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            select x.*, null, '' as "''", 1+2, $seq::int as n1
            from (
                select id, kind, 123::bigint, $kind::int as c
                from Foo
                where kind = $kind
            ) x
        )#");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("seq")}, 
            {"2", ParamLookupEntry("kind")}
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("duplicated named parameter#2 (NOT same type)") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            select x.*, null, '' as "''", 1+2, $seq::int as n1, $kind::int as k2
            from (
                select id, kind, 123::bigint, $kind::text || '_abc' as c
                from Foo
                where kind = $kind::int
            ) x
        )#");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("seq")}, 
            {"2", ParamLookupEntry("kind")}
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("named parameter in values") {
        std::string sql("values ($id_1::int, $name_1::text, $age_1::int), ($id_2::int, $name_2::text, $age_2::int), ");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("id_1")}, {"2", ParamLookupEntry("name_1")}, {"3", ParamLookupEntry("age_1")},
            {"4", ParamLookupEntry("id_2")}, {"5", ParamLookupEntry("name_2")}, {"6", ParamLookupEntry("age_2")}, 
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "VARCHAR"} }, 
            {"3", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"4", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"5", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "VARCHAR"} }, 
            {"6", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {}, lookup, bound_types, user_type_names, anon_types);
    }
}

TEST_CASE("ResolveParam::table function") {
    SECTION("range") {
        std::string sql(R"#(
            select id from range(0, "$stop" := 20::bigint, "$step" := 3) t(id)
        )#");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("stop")},
            {"2", ParamLookupEntry("step")}
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "BIGINT"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {}, lookup, bound_types, user_type_names, anon_types);
    }
}

TEST_CASE("ResolveParam::window function") {
    SECTION("function args") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select id, sum($val::bigint) over () as a from Foo");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("val")}, 
        };
        ExpectParamLookup bound_types{{"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "BIGINT"} }};
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("patrition by") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select id, sum($val::bigint) over (partition by fmod(id, $rem::int)) as a from Foo");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("val")},
            {"2", ParamLookupEntry("rem")} 
        };
        ExpectParamLookup bound_types{ 
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "BIGINT"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} } 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("order by") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select id, sum($val::double) over (order by fmod(id, $rem::int)) as a from Foo");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("val")},
            {"2", ParamLookupEntry("rem")} 
        };
        ExpectParamLookup bound_types{ 
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "DOUBLE"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} } 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("filter") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select id, sum($val::bigint) filter (fmod(id, $div::int) > $rem::int) over () as a from Foo");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("val")},
            {"2", ParamLookupEntry("div")},
            {"3", ParamLookupEntry("rem")} 
        };
        ExpectParamLookup bound_types{ 
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "BIGINT"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"3", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} } 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("qualify") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            select * 
            from Foo
            qualify sum($val::int) over () > 100
        )#");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("val")},
        };
        ExpectParamLookup bound_types{ {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} } };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("frame#1 (rows)") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            select id, 
                sum($val::int) 
                over (
                    rows between $from_row preceding and $to_row following
                ) as a
            from Foo
        )#");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("val")},
            {"2", ParamLookupEntry("from_row")},
            {"3", ParamLookupEntry("to_row")},
        };
        ExpectParamLookup bound_types{ 
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "BIGINT"} }, 
            {"3", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "BIGINT"} } 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("frame#2 (range)") {
        std::string schema("CREATE TABLE Temperature (id int primary key, y int not null, month_of_y int not null, record_at DATE not null, temperature FLOAT not null)");
        std::string sql(R"#(
            select y, month_of_y, record_at, 
                avg(temperature) 
                over (
                    partition by y, month_of_y
                    order by record_at
                    range between interval ($days::int) days preceding and current row
                ) as a
            from Temperature
        )#");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("days")},
        };
        ExpectParamLookup bound_types{ {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} } };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
}

TEST_CASE("ResolveParam::builtin window function") {
    SECTION("ntile") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select id, ntile($bucket) over () as a from Foo");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("bucket")},
        };
        ExpectParamLookup bound_types{ {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "BIGINT"} } };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("lag") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select id, lag(id, $offset, $value_def::int) over (partition by kind) as a from Foo");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("offset")},
            {"2", ParamLookupEntry("value_def")},
        };
        ExpectParamLookup bound_types{ 
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "BIGINT"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} } 
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
}

TEST_CASE("ResolveParam::user type#1 (ENUM)") {
    SECTION("anonymous/select-list") {
        std::string schema("CREATE TYPE Visibility as ENUM ('hide','visible')");
        std::string sql("select $vis::enum('hide','visible') as vis");

        ParamNameLookup lookup{
            {"1", ParamLookupEntry("vis")}
        };
        ExpectParamLookup bound_types{{"1", ExpectParam{.type_kind = UserTypeKind::Enum, .type_name = "Param::Enum#1"} }};
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{
            {.kind = UserTypeKind::Enum, .name = "Param::Enum#1", .fields = { 
                UserTypeEntry::Member("hide"), 
                UserTypeEntry::Member("visible") 
            }},
        };

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("predefined/select-list") {
        std::string schema("CREATE TYPE Visibility as ENUM ('hide','visible')");
        std::string sql("select $vis::Visibility as vis");

        ParamNameLookup lookup{
            {"1", ParamLookupEntry("vis")}
        };
        ExpectParamLookup bound_types{{"1", ExpectParam{.type_kind = UserTypeKind::User, .type_name = "Visibility"} }};
        UserTypeExpects user_type_names{"Visibility"};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("anonymous/where#1") {
        std::string schema_1("CREATE TYPE Visibility as ENUM ('hide','visible')");
        std::string schema_2("CREATE TABLE Control (id INTEGER primary key, name VARCHAR not null, vis ENUM('hide', 'visible') not null)");
        std::string sql("select * from Control where vis = $vis::ENUM('hide', 'visible')");

        ParamNameLookup lookup{
            {"1", ParamLookupEntry("vis")}
        };
        ExpectParamLookup bound_types{{"1", ExpectParam{.type_kind = UserTypeKind::Enum, .type_name = "Param::Enum#1"} }};
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{
            {.kind = UserTypeKind::Enum, .name = "Param::Enum#1", .fields = { 
                UserTypeEntry::Member("hide"), 
                UserTypeEntry::Member("visible") 
            }},
        };

        runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("anonymous/where#2") {
        std::string schema_1("CREATE TYPE Visibility as ENUM('hide','visible')");
        std::string schema_2("CREATE TABLE Control (id INTEGER primary key, name VARCHAR not null, vis Visibility not null)");
        std::string sql("select * from Control where vis = $vis::ENUM('hide', 'visible')");

        ParamNameLookup lookup{
            {"1", ParamLookupEntry("vis")}
        };
        ExpectParamLookup bound_types{{"1", ExpectParam{.type_kind = UserTypeKind::Enum, .type_name = "Param::Enum#1"} }};
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{
            {.kind = UserTypeKind::Enum, .name = "Param::Enum#1", .fields = { 
                UserTypeEntry::Member("hide"), 
                UserTypeEntry::Member("visible") 
            }},
        };

        runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("predefined/where") {
        std::string schema_1("CREATE TYPE Visibility as ENUM('hide','visible')");
        std::string schema_2("CREATE TABLE Control (id INTEGER primary key, name VARCHAR not null, vis Visibility not null)");
        std::string sql("select * from Control where vis = $vis::Visibility");

        ParamNameLookup lookup{
            {"1", ParamLookupEntry("vis")}
        };
        ExpectParamLookup bound_types{{"1", ExpectParam{.type_kind = UserTypeKind::User, .type_name = "Visibility"} }};
        UserTypeExpects user_type_names{"Visibility"};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, user_type_names, anon_types);
    }
}

TEST_CASE("ResolveParam::CTE") {
    SECTION("not materialized CTE") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            with ph as not materialized (
                select $a::int as a, $b::text as b, kind from Foo
            )
            select b, a from ph
            where kind = $k
        )#");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("a")}, 
            {"2", ParamLookupEntry("b")}, 
            {"3", ParamLookupEntry("k")}
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "VARCHAR"} }, 
            {"3", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};
    
        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("default CTE") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            with ph as (
                select $a::int as a, $b::text as b, kind from Foo
            )
            select b, a from ph
            where kind = $k
        )#");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("a")}, 
            {"2", ParamLookupEntry("b")}, 
            {"3", ParamLookupEntry("k")}
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "VARCHAR"} }, 
            {"3", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }};
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};
    
        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
}

TEST_CASE("ResolveParam::materialized CTE") {
    SECTION("basic") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            with ph as materialized (
                select $a::int as a, $b::text as b, kind from Foo
            )
            select b, a from ph
            where kind = $k
        )#");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("a")}, 
            {"2", ParamLookupEntry("b")}, 
            {"3", ParamLookupEntry("k")}
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "VARCHAR"} }, 
            {"3", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }};
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};
    
        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("CTEx2") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int, value VARCHAR not null)");
        std::string schema_3("CREATE TABLE Point (id int, x int not null, y int, z int not null)");
        std::string sql(R"#(
            with
                v as materialized (
                    select Foo.id, Bar.id, xys, kind, a from Foo
                    join Bar on Foo.id = Bar.id
                    cross join (
                        select $a::int as a
                    )
                ),
                v2 as materialized (
                    select $b::text as b, x from Point
                )
            select xys, id, b, x, a from v
            cross join v2
        )#");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("a")}, 
            {"2", ParamLookupEntry("b")}
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "VARCHAR"} }
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};
    
        runResolveParamType(sql, {schema_1, schema_2, schema_3}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("nested") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int, value VARCHAR not null)");
        std::string sql(R"#(
            with
                v as materialized (
                    select Foo.id, Bar.id, xys, kind, a from Foo
                    join Bar on Foo.id = Bar.id
                    cross join (
                        select $a::int as a
                    )
                ),
                v2 as materialized (
                    select id, id_1, $b::date from v
                )
            select id_1, id from v2
        )#");
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("a")}, 
            {"2", ParamLookupEntry("b")}
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "DATE"} }
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};
    
        runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, user_type_names, anon_types);
    }
}

TEST_CASE("ResolveParam::Recursive CTE") {
    SECTION("default CTE") {
        std::string sql(R"#(
            with recursive t(n) AS (
                VALUES ($min_value::int)
                UNION ALL
                SELECT n+1 FROM t WHERE n < $max_value::bigint
            )
            SELECT n FROM t
        )#");

        ParamNameLookup lookup{
            {"1", ParamLookupEntry("min_value")}, 
            {"2", ParamLookupEntry("max_value")}
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "BIGINT"} }
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};
    
        runResolveParamType(sql, {}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("default CTEx2") {
        std::string sql(R"#(
            with recursive 
                t(n) AS (
                    VALUES ($min_value::int)
                    UNION ALL
                    SELECT n+1 FROM t WHERE n < $max_value::int
                ),
                t2(m) AS (
                    VALUES ($min_value::int)
                    UNION ALL
                    SELECT m*2 FROM t2 WHERE m < $max_value2::bigint
                )
            SELECT n, m FROM t positional join t2
        )#");

        ParamNameLookup lookup{
            {"1", ParamLookupEntry("min_value")}, 
            {"2", ParamLookupEntry("max_value")},
            {"3", ParamLookupEntry("max_value2")}
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"3", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "BIGINT"} }
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};
    
        runResolveParamType(sql, {}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("default CTE (nested)") {
        std::string sql(R"#(
            with recursive
                t(n) AS (
                    VALUES ($min_value::int)
                    UNION ALL
                    SELECT n+1 FROM t WHERE n < $max_value::bigint
                ),
                t2(m) as (
                    select n + $delta::int as m from t
                    union all
                    select m*2 from t2 where m < $max_value2::bigint
                )
            SELECT m FROM t2
        )#");

        ParamNameLookup lookup{
            {"1", ParamLookupEntry("min_value")}, 
            {"2", ParamLookupEntry("max_value")},
            {"3", ParamLookupEntry("delta")},
            {"4", ParamLookupEntry("max_value2")}
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "BIGINT"} }, 
            {"3", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"4", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "BIGINT"} }
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};
    
        runResolveParamType(sql, {}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("Not matterialized CTE") {
        std::string sql(R"#(
            with recursive t(n) AS not materialized (
                VALUES ($min_value::int)
                UNION ALL
                SELECT n+1 FROM t WHERE n < $max_value::bigint
            )
            SELECT n FROM t
        )#");

        ParamNameLookup lookup{
            {"1", ParamLookupEntry("min_value")}, 
            {"2", ParamLookupEntry("max_value")}
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "BIGINT"} }
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};
    
        runResolveParamType(sql, {}, lookup, bound_types, user_type_names, anon_types);
    }
}

TEST_CASE("ResolveParam::Recursive materialized CTE") {
    SECTION("Matirialized CTE") {
        std::string sql(R"#(
            with recursive t(n) AS materialized (
                VALUES ($min_value::bigint)
                UNION ALL
                SELECT n+1 FROM t WHERE n < $max_value::bigint
            )
            SELECT n FROM t
        )#");

        ParamNameLookup lookup{
            {"1", ParamLookupEntry("min_value")}, 
            {"2", ParamLookupEntry("max_value")}
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "BIGINT"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "BIGINT"} }
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};
    
        runResolveParamType(sql, {}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("Matirialized CTEx2") {
        std::string sql(R"#(
            with recursive 
                t(n) AS materialized (
                    VALUES ($min_value::int)
                    UNION ALL
                    SELECT n+1 FROM t WHERE n < $max_value::int
                ),
                t2(m) AS materialized (
                    VALUES ($min_value::int)
                    UNION ALL
                    SELECT m*2 FROM t2 WHERE m < $max_value2::bigint
                )
            SELECT n, m FROM t positional join t2
        )#");

        ParamNameLookup lookup{
            {"1", ParamLookupEntry("min_value")}, 
            {"2", ParamLookupEntry("max_value")},
            {"3", ParamLookupEntry("max_value2")}
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"3", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "BIGINT"} }};
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};
    
        runResolveParamType(sql, {}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("materialized CTE (nested)") {
        std::string sql(R"#(
            with recursive
                t(n) AS materialized (
                    VALUES ($min_value::int)
                    UNION ALL
                    SELECT n+1 FROM t WHERE n < $max_value::bigint
                ),
                t2(m) as materialized (
                    select n + $delta::float as m from t
                    union all
                    select m*2 from t2 where m < $max_value2::bigint
                )
            SELECT m FROM t2
        )#");

        ParamNameLookup lookup{
            {"1", ParamLookupEntry("min_value")}, 
            {"2", ParamLookupEntry("max_value")},
            {"3", ParamLookupEntry("delta")},
            {"4", ParamLookupEntry("max_value2")}
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "BIGINT"} }, 
            {"3", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "FLOAT"} }, 
            {"4", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "BIGINT"} }
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};
    
        runResolveParamType(sql, {}, lookup, bound_types, user_type_names, anon_types);
    }
}

TEST_CASE("ResolveParam::combining operation") {
    SECTION("basic") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int, value VARCHAR not null)");
        std::string sql(R"#(
            select id from Foo where id > $n1
            union all
            select id from Bar where id <= $n2
        )#");

        ParamNameLookup lookup{
            {"1", ParamLookupEntry("n1")}, 
            {"2", ParamLookupEntry("n2")}
        };
        ExpectParamLookup bound_types{
            {"1", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }, 
            {"2", ExpectParam{.type_kind = UserTypeKind::Primitive, .type_name = "INTEGER"} }
        };
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};
    
        runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, user_type_names, anon_types);
    }
}

#endif
