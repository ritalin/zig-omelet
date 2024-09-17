#include <algorithm>

#include <duckdb.hpp>
#include <duckdb/planner/logical_operator_visitor.hpp>
#include <duckdb/planner/expression/list.hpp>

#include <magic_enum/magic_enum.hpp>

#include "duckdb_params_collector.hpp"
#include "duckdb_binder_support.hpp"

namespace worker {

class LogicalParameterVisitor: public duckdb::LogicalOperatorVisitor {
public:
    LogicalParameterVisitor(ParamNameLookup&& names_ref): names(names_ref) {}
public:
    auto VisitResult() -> ParamResolveResult;
protected:
	auto VisitReplace(duckdb::BoundParameterExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
private:
    auto hasAnyType(const PositionalParam& position, const std::string& type_name) -> bool;
private:
    ParamNameLookup names;
    std::unordered_multimap<PositionalParam, std::string> param_types;
    std::unordered_map<PositionalParam, ParamEntry> parameters;
    std::vector<std::string> user_type_names;
    std::vector<UserTypeEntry> anon_types;
};

auto LogicalParameterVisitor::VisitResult() -> ParamResolveResult {
    auto view = this->parameters | std::views::values;
    
    std::vector<ParamEntry> params(view.begin(), view.end());
    std::ranges::sort(params, {}, &ParamEntry::sort_order);

    return {
        .params = std::move(params), 
        .user_type_names = std::move(this->user_type_names),
        .anon_types = std::move(this->anon_types)
    };
}

static inline auto hasAnyTypeInternal(const std::unordered_multimap<PositionalParam, std::string>& param_types, const PositionalParam& position, const std::string& type_name) -> bool {
    auto [iter_from, itr_to] = param_types.equal_range(position);
    auto view = std::ranges::subrange(iter_from, itr_to);

    auto pred_fn = [&type_name](std::pair<std::string, std::string> p) { 
        auto [k, v] = p;
        return v == type_name; 
    };
    if (std::ranges::any_of(view, pred_fn)) {
        return false;
    }

    return true;
}

auto LogicalParameterVisitor::hasAnyType(const PositionalParam& position, const std::string& type_name) -> bool {
    return hasAnyTypeInternal(this->param_types, position, type_name);
}

auto LogicalParameterVisitor::VisitReplace(duckdb::BoundParameterExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    std::string type_name;

    if (isEnumUserType(expr.return_type)) {
        type_name = userTypeName(expr.return_type);
        if (type_name != "") {
            this->user_type_names.push_back(type_name);
        }
        else {
            // process as anonymous user type
            type_name = std::format("Param::{}#{}", magic_enum::enum_name(expr.return_type.id()), expr.identifier);
            this->anon_types.push_back(pickEnumUserType(expr.return_type, type_name));
        }
    }
    else {
        type_name = expr.return_type.ToString();
    }
    
    if (! this->parameters.contains(expr.identifier)) {
        this->parameters[expr.identifier] = ParamEntry{
            .position = expr.identifier,
            .name = this->names.at(expr.identifier),
            .type_name = type_name,
            .sort_order = std::stoul(expr.identifier)
        };
        this->param_types.insert(std::make_pair(expr.identifier, type_name));

        return nullptr;
    }

    if (this->hasAnyType(expr.identifier, type_name)) {
        this->parameters.at(expr.identifier).type_name = std::nullopt;
        this->param_types.insert(std::make_pair(expr.identifier, type_name));
    }

    return nullptr;
}

auto resolveParamType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ParamNameLookup&& name_lookup) -> ParamResolveResult {
    LogicalParameterVisitor visitor(std::move(name_lookup));
    visitor.VisitOperator(*op);

    return visitor.VisitResult();
}

}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include "duckdb_database.hpp"

using namespace worker;
using namespace Catch::Matchers;

using ParamTypeLookup = std::unordered_map<NamedParam, std::string>;
using UserTypeExpects = std::vector<std::string>;
using AnonTypeExpects = std::vector<UserTypeEntry>;

TEST_CASE("Typename checking#1 (same type)") {
    std::unordered_multimap<std::string, std::string> m{{"1","INTEGER"},{"2","VARCHAR"}};

    SECTION("Supporse a same type") {
        REQUIRE_FALSE(hasAnyTypeInternal(m, "1", "INTEGER"));
    }
    SECTION("Supporse a ANY type") {
        REQUIRE(hasAnyTypeInternal(m, "1", "VARCHAR"));
    }
}

static auto expectAnonymousUserType(const UserTypeEntry& actual, const UserTypeEntry& expect, size_t i, size_t depth) -> void {
    anon_type_kind: {
        UNSCOPED_INFO(std::format("[{}] anon type kind#{}", depth, i));
        CHECK(actual.kind == expect.kind);
    }
    anon_type_name: {
        UNSCOPED_INFO(std::format("[{}] anon type named#{}", depth, i));
        CHECK_THAT(actual.name, Equals(expect.name));
    }
    anon_type_field_entries: {
        anonymous_type_field_size: {
            UNSCOPED_INFO(std::format("[{}] Parameter size#{}", depth, i));
            REQUIRE(actual.fields.size() == expect.fields.size());
        }

        auto field_view = expect.fields | std::views::transform([](auto f) {
            return std::pair<std::string, UserTypeEntry::Member>(f.field_name, f);
        });
        std::unordered_map<std::string, UserTypeEntry::Member> field_lookup(field_view.begin(), field_view.end());

        for (int j = 0; auto& field: actual.fields) {
            has_anon_field: {
                UNSCOPED_INFO(std::format("[{}] valid field named#{} of anon type#{}", depth, j, i));
                REQUIRE(field_lookup.contains(field.field_name));
            }

            auto& expect_field = field_lookup.at(field.field_name);

            anon_type_field_name: {
                UNSCOPED_INFO(std::format("[{}] field named#{} of anon type#{}", depth, j, i));
                CHECK_THAT(field.field_name, Equals(expect_field.field_name));
            }
            anon_type_field_type: {
                UNSCOPED_INFO(std::format("field type#{} of anon type#{}", j, i));
                CHECK((bool)field.field_type == (bool)expect_field.field_type);

                if (field.field_type) {
                    expectAnonymousUserType(*field.field_type, *expect_field.field_type, i, depth+1);
                }
            }
            ++j;
        }
    }
}

static auto runResolveParamType(
    const std::string& sql, const std::vector<std::string>& schemas, 
    const ParamNameLookup& name_lookup, const ParamTypeLookup& param_types, 
    UserTypeExpects user_type_expects, AnonTypeExpects anon_type_expects) -> void 
{
    auto db = Database();
    auto conn = db.connect();

    prepare_schema: {
        for (auto& schema: schemas) {
            conn.Query(schema);
        }
        db.retainUserTypeName(conn);
    }

    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];

    duckdb::case_insensitive_map_t<duckdb::BoundParameterData> parameter_map{};
    duckdb::BoundParameterMap parameters(parameter_map);
    
    auto binder = duckdb::Binder::CreateBinder(*conn.context);
    binder->SetCanContainNulls(true);
    binder->parameters = &parameters;

    duckdb::BoundStatement bind_result;
    ParamCollectionResult walk_result;
    try {
        conn.BeginTransaction();
        walk_result = walkSQLStatement(stmt, ZmqChannel::unitTestChannel());
        bind_result = binder->Bind(*stmt->Copy());
        conn.Commit();
    }
    catch (...) {
        conn.Rollback();
        throw;
    }

    auto [resolve_result, user_type_names, anon_types] = resolveParamType(bind_result.plan, std::move(walk_result.names));

    parameter_size: {
        UNSCOPED_INFO("Parameter size");
        REQUIRE(resolve_result.size() == name_lookup.size());
    }
    parameter_entries: {
        for (int i = 0; auto& entry: resolve_result) {
            has_param: {
                UNSCOPED_INFO(std::format("valid named parameter#{}", i));
                REQUIRE(name_lookup.contains(entry.position));
                REQUIRE_THAT(entry.name, Equals(name_lookup.at(entry.position)));
            }

            if (param_types.contains(entry.position)) {
                with_type_param: {
                    UNSCOPED_INFO(std::format("valid parameter type#{} (has type)", i));
                    REQUIRE(param_types.contains(entry.position));
                    REQUIRE_THAT(entry.type_name.value(), Equals(param_types.at(entry.position)));
                }
            }
            else {
                without_type_param: {
                    UNSCOPED_INFO(std::format("valid parameter type#{} (has ANY type)", i));
                    REQUIRE_FALSE((bool)entry.type_name);
                }
            }
            ++i;
        }
    }
    user_type_entries: {
        user_type_size: {
            UNSCOPED_INFO("User type size (predefined)");
            REQUIRE(user_type_names.size() == user_type_expects.size());
        }

        std::unordered_set<std::string> lookup(user_type_expects.begin(), user_type_expects.end());

        for (int i = 0; auto& name: user_type_names) {
            has_user_type: {
                UNSCOPED_INFO(std::format("valid user type named#{}", i));
                REQUIRE(lookup.contains(name));   
            }
            ++i;
        }
    }
    anonymous_entries: {
        anonymous_type_size: {
            UNSCOPED_INFO("User type size (anonymous)");
            REQUIRE(anon_types.size() == anon_type_expects.size());
        }

        auto view = anon_type_expects | std::views::transform([](auto x) {
            return std::pair<std::string, UserTypeEntry>(x.name, x);
        });
        std::unordered_map<std::string, UserTypeEntry> lookup(view.begin(), view.end());

        for (int i = 0; auto& entry: anon_types) {
            has_anon_type: {
                UNSCOPED_INFO(std::format("valid anon type named#{}", i));
                REQUIRE(lookup.contains(entry.name));
            }

            expectAnonymousUserType(entry, lookup.at(entry.name), i++, 0);
        }
    }
}

TEST_CASE("ResolveParam::without parameters") {
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string sql("select * from Foo");
    ParamNameLookup lookup{};
    ParamTypeLookup bound_types{};
    UserTypeExpects user_type_names{};
    AnonTypeExpects anon_types{};

    runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
}

TEST_CASE("ResolveParam::positional parameter") {
    SECTION("where clause") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select * from Foo where kind = $1");
        ParamNameLookup lookup{{"1","1"}};
        ParamTypeLookup bound_types{{"1","INTEGER"}};
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("select list and where clause") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select id, $2::text from Foo where kind = $1");
        ParamNameLookup lookup{{"1","1"}, {"2","2"}};
        ParamTypeLookup bound_types{{"1","INTEGER"}, {"2","VARCHAR"}};
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
}

TEST_CASE("ResolveParam::named parameter") {
    SECTION("where clause") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select * from Foo where kind = $kind");
        ParamNameLookup lookup{{"1","kind"}};
        ParamTypeLookup bound_types{{"1","INTEGER"}};
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("select list and where clause") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select id, $phrase::text from Foo where kind = $kind");
        ParamNameLookup lookup{{"1","phrase"}, {"2","kind"}};
        ParamTypeLookup bound_types{{"1","VARCHAR"}, {"2","INTEGER"}};
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
        ParamNameLookup lookup{{"1","phrase"}, {"2", "serch_word"}, {"3","kind"}};
        ParamTypeLookup bound_types{{"1","VARCHAR"}, {"2", "VARCHAR"}, {"3","INTEGER"}};
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
        ParamNameLookup lookup{{"1","seq"}, {"2", "phrase"}, {"3","kind"}};
        ParamTypeLookup bound_types{{"1","INTEGER"}, {"2", "VARCHAR"}, {"3","INTEGER"}};
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
        ParamNameLookup lookup{{"1","seq"}, {"2","kind"}};
        ParamTypeLookup bound_types{{"1","INTEGER"}, {"2", "INTEGER"}};
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
        ParamNameLookup lookup{{"1","seq"}, {"2","kind"}};
        ParamTypeLookup bound_types{{"1","INTEGER"}};
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
}

TEST_CASE("ResolveParam::user type#1 (ENUM)") {
    SECTION("anonymous/select-list") {
        std::string schema("CREATE TYPE Visibility as ENUM ('hide','visible')");
        std::string sql("select $vis::enum('hide','visible') as vis");

        ParamNameLookup lookup{{"1","vis"}};
        ParamTypeLookup bound_types{{"1","Param::ENUM#1"}};
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{
            {.kind = UserTypeKind::Enum, .name = "Param::ENUM#1", .fields = { UserTypeEntry::Member("hide"), UserTypeEntry::Member("visible") }},
        };

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("predefined/select-list") {
        std::string schema("CREATE TYPE Visibility as ENUM ('hide','visible')");
        std::string sql("select $vis::Visibility as vis");

        ParamNameLookup lookup{{"1","vis"}};
        ParamTypeLookup bound_types{{"1","Visibility"}};
        UserTypeExpects user_type_names{"Visibility"};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("anonymous/where#1") {
        std::string schema_1("CREATE TYPE Visibility as ENUM ('hide','visible')");
        std::string schema_2("CREATE TABLE Control (id INTEGER primary key, name VARCHAR not null, vis ENUM('hide', 'visible') not null)");
        std::string sql("select * from Control where vis = $vis::ENUM('hide', 'visible')");

        ParamNameLookup lookup{{"1","vis"}};
        ParamTypeLookup bound_types{{"1","Param::ENUM#1"}};
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{
            {.kind = UserTypeKind::Enum, .name = "Param::ENUM#1", .fields = { UserTypeEntry::Member("hide"), UserTypeEntry::Member("visible") }},
        };

        runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("anonymous/where#2") {
        std::string schema_1("CREATE TYPE Visibility as ENUM('hide','visible')");
        std::string schema_2("CREATE TABLE Control (id INTEGER primary key, name VARCHAR not null, vis Visibility not null)");
        std::string sql("select * from Control where vis = $vis::ENUM('hide', 'visible')");

        ParamNameLookup lookup{{"1","vis"}};
        ParamTypeLookup bound_types{{"1","Visibility"}};
        UserTypeExpects user_type_names{"Visibility"};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("predefined/where") {
        std::string schema_1("CREATE TYPE Visibility as ENUM('hide','visible')");
        std::string schema_2("CREATE TABLE Control (id INTEGER primary key, name VARCHAR not null, vis Visibility not null)");
        std::string sql("select * from Control where vis = $vis::Visibility");

        ParamNameLookup lookup{{"1","vis"}};
        ParamTypeLookup bound_types{{"1","Visibility"}};
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
        ParamNameLookup lookup{{"1","a"}, {"2", "b"}, {"3", "k"}};
        ParamTypeLookup bound_types{{"1","INTEGER"}, {"2", "VARCHAR"}, {"3", "INTEGER"}};
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
        ParamNameLookup lookup{{"1","a"}, {"2", "b"}, {"3", "k"}};
        ParamTypeLookup bound_types{{"1","INTEGER"}, {"2", "VARCHAR"}, {"3", "INTEGER"}};
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
        ParamNameLookup lookup{{"1","a"}, {"2", "b"}, {"3", "k"}};
        ParamTypeLookup bound_types{{"1","INTEGER"}, {"2", "VARCHAR"}, {"3", "INTEGER"}};
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
        ParamNameLookup lookup{{"1","a"}, {"2", "b"}};
        ParamTypeLookup bound_types{{"1","INTEGER"}, {"2", "VARCHAR"}};
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
        ParamNameLookup lookup{{"1","a"}, {"2", "b"}};
        ParamTypeLookup bound_types{{"1","INTEGER"}, {"2", "DATE"}};
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};
    
        runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, user_type_names, anon_types);
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

        ParamNameLookup lookup{{"1","n1"}, {"2", "n2"}};
        ParamTypeLookup bound_types{{"1","INTEGER"}, {"2", "INTEGER"}};
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};
    
        runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, user_type_names, anon_types);
    }
}

#endif
