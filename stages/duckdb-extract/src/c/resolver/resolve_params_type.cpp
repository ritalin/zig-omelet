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
    LogicalParameterVisitor(ParamNameLookup&& names_ref, BoundParamTypeHint&& type_hints_ref): names(std::move(names_ref)), type_hints(std::move(type_hints_ref)) {}
public:
    auto VisitResult() -> ParamResolveResult;
    auto paramFromExample(const ParamExampleLookup& examples) -> void;
protected:
	auto VisitReplace(duckdb::BoundParameterExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
private:
    auto hasAnyType(const PositionalParam& position, const std::string& type_name) -> bool;
private:
    ParamNameLookup names;
    BoundParamTypeHint type_hints;
private:
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

auto LogicalParameterVisitor::paramFromExample(const ParamExampleLookup& examples) -> void {
    auto example_view = examples | std::views::transform([this](const auto& pair) {
        auto positional = pair.first;
        auto& param_type = this->type_hints.contains(positional) ? this->type_hints.at(positional)->return_type : pair.second.value.type();

        ParamEntry entry{
            .position = positional,
            .name = this->names.at(positional).name,
            .type_name = param_type.ToString(),
            .sort_order = std::stoul(positional)
        };

        return std::pair(positional, entry);
    });

    this->parameters.insert(example_view.begin(), example_view.end());
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
        if (this->type_hints.contains(expr.identifier)) {
            type_name = type_hints.at(expr.identifier)->return_type.ToString();
        }
        else {
            type_name = expr.return_type.ToString();
        }
    }
    
    if (! this->parameters.contains(expr.identifier)) {
        this->parameters[expr.identifier] = ParamEntry{
            .position = expr.identifier,
            .name = this->names.at(expr.identifier).name,
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

auto resolveParamType(
    duckdb::unique_ptr<duckdb::LogicalOperator>& op, 
    ParamNameLookup&& name_lookup, 
    BoundParamTypeHint&& type_hints, 
    ParamExampleLookup&& examples) -> ParamResolveResult 
{
    LogicalParameterVisitor visitor(std::move(name_lookup), std::move(type_hints));
    visitor.paramFromExample(examples);
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

    BoundResult bound_result;
    ParamCollectionResult walk_result;
    try {
        conn.BeginTransaction();
        walk_result = walkSQLStatement(stmt, ZmqChannel::unitTestChannel());
        bound_result = bindTypeToStatement(*conn.context, std::move(stmts[0]->Copy()), walk_result.names, walk_result.examples);
        conn.Commit();
    }
    catch (...) {
        conn.Rollback();
        throw;
    }

    auto [resolve_result, user_type_names, anon_types] = resolveParamType(
        bound_result.stmt.plan, 
        std::move(walk_result.names), 
        std::move(bound_result.type_hints),
        std::move(walk_result.examples)
    );

    parameter_size: {
        INFO("Parameter size");
        REQUIRE(resolve_result.size() == name_lookup.size());
    }
    parameter_entries: {
        for (int i = 0; auto& entry: resolve_result) {
            has_param: {
                INFO(std::format("valid named parameter#{}", i+1));
                REQUIRE(name_lookup.contains(entry.position));
                REQUIRE_THAT(entry.name, Equals(name_lookup.at(entry.position).name));
            }

            if (param_types.contains(entry.position)) {
                with_type_param: {
                    INFO(std::format("valid parameter type#{} (has type)", i+1));
                    REQUIRE(param_types.contains(entry.position));
                    REQUIRE_THAT(entry.type_name.value(), Equals(param_types.at(entry.position)));
                }
            }
            else {
                without_type_param: {
                    INFO(std::format("valid parameter type#{} (has ANY type)", i+1));
                    REQUIRE_FALSE((bool)entry.type_name);
                }
            }
            ++i;
        }
    }
    user_type_entries: {
        user_type_size: {
            INFO("User type size (predefined)");
            REQUIRE(user_type_names.size() == user_type_expects.size());
        }

        std::unordered_set<std::string> lookup(user_type_expects.begin(), user_type_expects.end());

        for (int i = 0; auto& name: user_type_names) {
            has_user_type: {
                INFO(std::format("valid user type named#{}", i+1));
                REQUIRE(lookup.contains(name));   
            }
            ++i;
        }
    }
    anonymous_entries: {
        anonymous_type_size: {
            INFO("User type size (anonymous)");
            REQUIRE(anon_types.size() == anon_type_expects.size());
        }

        auto view = anon_type_expects | std::views::transform([](auto x) {
            return std::pair<std::string, UserTypeEntry>(x.name, x);
        });
        std::unordered_map<std::string, UserTypeEntry> lookup(view.begin(), view.end());

        for (int i = 0; auto& entry: anon_types) {
            has_anon_type: {
                INFO(std::format("valid anon type named#{}", i+1));
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
        ParamNameLookup lookup{
            { "1", ParamLookupEntry("1") }
        };
        ParamTypeLookup bound_types{{"1","INTEGER"}};
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
        ParamTypeLookup bound_types{ {"1", "INTEGER"}, {"2", "VARCHAR"} };
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
        ParamTypeLookup bound_types{ {"1","INTEGER"}, {"2","INTEGER"}, {"3","INTEGER"} };
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
        ParamTypeLookup bound_types{{"1","INTEGER"}};
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
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("phrase")}, 
            {"2", ParamLookupEntry("serch_word")}, 
            {"3", ParamLookupEntry("kind")}};
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
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("seq")}, 
            {"2", ParamLookupEntry("phrase")}, 
            {"3", ParamLookupEntry("kind")}
        };
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
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("seq")}, 
            {"2", ParamLookupEntry("kind")}
        };
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
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("seq")}, 
            {"2", ParamLookupEntry("kind")}
        };
        ParamTypeLookup bound_types{{"1","INTEGER"}, {"2", "INTEGER"}};
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema}, lookup, bound_types, user_type_names, anon_types);
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
        ParamTypeLookup bound_types{{"1","BIGINT"}, {"2","INTEGER"}};
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
        ParamTypeLookup bound_types{{"1","BIGINT"}};
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
        ParamTypeLookup bound_types{ {"1","BIGINT"}, {"2","INTEGER"} };
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
        ParamTypeLookup bound_types{ {"1","DOUBLE"}, {"2","INTEGER"} };
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
        ParamTypeLookup bound_types{ {"1","BIGINT"}, {"2","INTEGER"}, {"3","INTEGER"} };
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
        ParamTypeLookup bound_types{ {"1","INTEGER"} };
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
        ParamTypeLookup bound_types{ {"1","INTEGER"}, {"2","BIGINT"}, {"3","BIGINT"} };
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
        ParamTypeLookup bound_types{ {"1","INTEGER"} };
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
        ParamTypeLookup bound_types{ {"1","BIGINT"} };
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
        ParamTypeLookup bound_types{ {"1","BIGINT"}, {"2","INTEGER"} };
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

        ParamNameLookup lookup{
            {"1", ParamLookupEntry("vis")}
        };
        ParamTypeLookup bound_types{{"1","Visibility"}};
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

        ParamNameLookup lookup{
            {"1", ParamLookupEntry("vis")}
        };
        ParamTypeLookup bound_types{{"1","Visibility"}};
        UserTypeExpects user_type_names{"Visibility"};
        AnonTypeExpects anon_types{};

        runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, user_type_names, anon_types);
    }
    SECTION("predefined/where") {
        std::string schema_1("CREATE TYPE Visibility as ENUM('hide','visible')");
        std::string schema_2("CREATE TABLE Control (id INTEGER primary key, name VARCHAR not null, vis Visibility not null)");
        std::string sql("select * from Control where vis = $vis::Visibility");

        ParamNameLookup lookup{
            {"1", ParamLookupEntry("vis")}
        };
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
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("a")}, 
            {"2", ParamLookupEntry("b")}, 
            {"3", ParamLookupEntry("k")}
        };
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
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("a")}, 
            {"2", ParamLookupEntry("b")}, 
            {"3", ParamLookupEntry("k")}
        };
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
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("a")}, 
            {"2", ParamLookupEntry("b")}, 
            {"3", ParamLookupEntry("k")}
        };
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
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("a")}, 
            {"2", ParamLookupEntry("b")}
        };
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
        ParamNameLookup lookup{
            {"1", ParamLookupEntry("a")}, 
            {"2", ParamLookupEntry("b")}
        };
        ParamTypeLookup bound_types{{"1","INTEGER"}, {"2", "DATE"}};
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
        ParamTypeLookup bound_types{{"1","INTEGER"}, {"2", "BIGINT"}};
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
        ParamTypeLookup bound_types{{"1","INTEGER"}, {"2","INTEGER"}, {"3", "BIGINT"}};
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
        ParamTypeLookup bound_types{{"1","INTEGER"}, {"2","BIGINT"}, {"3","INTEGER"}, {"4", "BIGINT"}};
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
        ParamTypeLookup bound_types{{"1","INTEGER"}, {"2", "BIGINT"}};
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
        ParamTypeLookup bound_types{{"1","BIGINT"}, {"2", "BIGINT"}};
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
        ParamTypeLookup bound_types{{"1","INTEGER"}, {"2","INTEGER"}, {"3", "BIGINT"}};
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
        ParamTypeLookup bound_types{{"1","INTEGER"}, {"2","BIGINT"}, {"3","FLOAT"}, {"4", "BIGINT"}};
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
        ParamTypeLookup bound_types{{"1","INTEGER"}, {"2", "INTEGER"}};
        UserTypeExpects user_type_names{};
        AnonTypeExpects anon_types{};
    
        runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, user_type_names, anon_types);
    }
}

#endif
