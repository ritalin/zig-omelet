#include <algorithm>

#include <duckdb.hpp>
#include <duckdb/planner/logical_operator_visitor.hpp>
#include <duckdb/planner/expression/list.hpp>
#include <duckdb/common/extra_type_info.hpp>
#include <duckdb/common/types/vector.hpp>

#include <magic_enum/magic_enum.hpp>

#include "duckdb_params_collector.hpp"
#include "duckdb_binder_support.hpp"

// #include <duckdb/parser/query_node/list.hpp>
// #include <duckdb/parser/expression/list.hpp>
// #include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>

namespace worker {

class LogicalParameterVisitor: public duckdb::LogicalOperatorVisitor {
public:
    LogicalParameterVisitor(ParamNameLookup&& names_ref, const UserTypeLookup<PositionalParam>& user_types_ref): names(names_ref), user_types(user_types_ref) {}
public:
    auto VisitResult() -> ParamResolveResult;
protected:
	auto VisitReplace(duckdb::BoundParameterExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
private:
    auto hasAnyType(const PositionalParam& position, const std::string& type_name) -> bool;
private:
    ParamNameLookup names;
    UserTypeLookup<PositionalParam> user_types;
    std::unordered_map<PositionalParam, ParamEntry> parameters;
    std::unordered_multimap<PositionalParam, std::string> param_types;
    std::vector<UserTypeEntry> anon_types;
};

auto LogicalParameterVisitor::VisitResult() -> ParamResolveResult {
    auto view = this->parameters | std::views::values;
    
    std::vector<ParamEntry> params(view.begin(), view.end());
    std::ranges::sort(params, {}, &ParamEntry::sort_order);

    return {.params = std::move(params), .anon_types = std::move(this->anon_types)};
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

static auto collectAnonymousUserType(const duckdb::BoundParameterExpression &expr, const std::string type_name) -> UserTypeEntry {
    auto *ext_info = expr.return_type.AuxInfo();

    std::vector<UserTypeEntry::Member> fields;
    if (ext_info->type == duckdb::ExtraTypeInfoType::ENUM_TYPE_INFO) {
        auto& enum_ext_info = ext_info->Cast<duckdb::EnumTypeInfo>();
        auto values = duckdb::FlatVector::GetData<duckdb::string_t>(enum_ext_info.GetValuesInsertOrder());
        auto size = enum_ext_info.GetDictSize();;
        
        for (auto iter = values; iter != values + size; ++iter) {
            fields.push_back({.field_name = iter->GetString()});
        }
    }

    return {
        .kind = UserTypeKind::Enum,
        .name = type_name,
        .fields = fields,
    };
}

static std::unordered_set<duckdb::LogicalTypeId> user_type_id_set{duckdb::LogicalTypeId::ENUM};

auto LogicalParameterVisitor::VisitReplace(duckdb::BoundParameterExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    std::string type_name;
    if (user_type_id_set.contains(expr.return_type.id())) {
        if (this->names.contains(expr.identifier) && this->user_types.contains(expr.identifier)) {
            type_name = this->user_types.at(expr.identifier);
        }
        else {
            // process as anonymous user type
            type_name = std::format("{}#{}", magic_enum::enum_name(expr.return_type.id()), expr.identifier);
            this->anon_types.push_back(collectAnonymousUserType(expr, type_name));
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

auto resolveParamType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ParamNameLookup&& name_lookup, const UserTypeLookup<PositionalParam>& user_type_lookup) -> ParamResolveResult {
    LogicalParameterVisitor visitor(std::move(name_lookup), std::move(user_type_lookup));
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

using namespace worker;
using namespace Catch::Matchers;

using ParamTypeLookup = std::unordered_map<NamedParam, std::string>;
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

static auto runResolveParamType(
    const std::string& sql, const std::vector<std::string>& schemas, 
    const ParamNameLookup& name_lookup, const ParamTypeLookup& param_types, AnonTypeExpects anon_type_expects) -> void 
{
    auto db = duckdb::DuckDB(nullptr);
    auto conn = duckdb::Connection(db);

    prepare_schema: {
        for (auto& schema: schemas) {
            conn.Query(schema);
        }
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
        walk_result = std::move(walkSQLStatement(stmt, ZmqChannel::unitTestChannel()));
        bind_result = binder->Bind(*stmt->Copy());
        conn.Commit();
    }
    catch (...) {
        conn.Rollback();
        throw;
    }

    auto [resolve_result, anon_types] = resolveParamType(bind_result.plan, std::move(walk_result.names), walk_result.param_user_types);

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
    anonymous_entries: {
        anonymous_type_size: {
            UNSCOPED_INFO("Parameter size");
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

            auto& expect = lookup[entry.name];

            anon_type_kind: {
                UNSCOPED_INFO(std::format("anon type kind#{}", i));
                CHECK(entry.kind == expect.kind);
            }
            anon_type_name: {
                UNSCOPED_INFO(std::format("anon type named#{}", i));
                CHECK_THAT(entry.name, Equals(expect.name));
            }
            anon_type_field_entries: {
                anonymous_type_field_size: {
                    UNSCOPED_INFO("Parameter size");
                    REQUIRE(entry.fields.size() == expect.fields.size());
                }

                auto field_view = expect.fields | std::views::transform([](auto f) {
                    return std::pair<std::string, UserTypeEntry::Member>(f.field_name, f);
                });
                std::unordered_map<std::string, UserTypeEntry::Member> field_lookup(field_view.begin(), field_view.end());

                for (int j = 0; auto& field: entry.fields) {
                    has_anon_field: {
                        UNSCOPED_INFO(std::format("valid field named#{} of anon type#{}", j, i));
                        REQUIRE(field_lookup.contains(field.field_name));
                    }

                    auto& expect_field = field_lookup[field.field_name];

                    anon_type_field_name: {
                        UNSCOPED_INFO(std::format("field named#{} of anon type#{}", j, i));
                        CHECK_THAT(field.field_name, Equals(expect_field.field_name));
                    }
                    anon_type_field_type: {
                        UNSCOPED_INFO(std::format("field type#{} of anon type#{}", j, i));
                        CHECK(field.field_type.has_value() == expect_field.field_type.has_value());

                        if (field.field_type.has_value()) {
                            CHECK_THAT(field.field_type.value(), Equals(expect_field.field_type.value()));
                        }
                    }
                    ++j;
                }
            }
            ++i;
        }
    }
}

TEST_CASE("Resolve without parameters") {
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string sql("select * from Foo");
    ParamNameLookup lookup{};
    ParamTypeLookup bound_types{};
    AnonTypeExpects anon_types{};

    runResolveParamType(sql, {schema}, lookup, bound_types, anon_types);
}

TEST_CASE("Resolve positional parameter on where clause") {
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string sql("select * from Foo where kind = $1");
    ParamNameLookup lookup{{"1","1"}};
    ParamTypeLookup bound_types{{"1","INTEGER"}};
    AnonTypeExpects anon_types{};

    runResolveParamType(sql, {schema}, lookup, bound_types, anon_types);
}

TEST_CASE("Resolve positional parameter on select list and where clause") {
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string sql("select id, $2::text from Foo where kind = $1");
    ParamNameLookup lookup{{"1","1"}, {"2","2"}};
    ParamTypeLookup bound_types{{"1","INTEGER"}, {"2","VARCHAR"}};
    AnonTypeExpects anon_types{};

    runResolveParamType(sql, {schema}, lookup, bound_types, anon_types);
}

TEST_CASE("Resolve named parameter on where clause") {
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string sql("select * from Foo where kind = $kind");
    ParamNameLookup lookup{{"1","kind"}};
    ParamTypeLookup bound_types{{"1","INTEGER"}};
    AnonTypeExpects anon_types{};

    runResolveParamType(sql, {schema}, lookup, bound_types, anon_types);
}

TEST_CASE("Resolve named parameter on select list and where clause") {
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string sql("select id, $phrase::text from Foo where kind = $kind");
    ParamNameLookup lookup{{"1","phrase"}, {"2","kind"}};
    ParamTypeLookup bound_types{{"1","VARCHAR"}, {"2","INTEGER"}};
    AnonTypeExpects anon_types{};

    runResolveParamType(sql, {schema}, lookup, bound_types, anon_types);
}

TEST_CASE("Resolve named parameters on joined select list and where clause") {
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
    AnonTypeExpects anon_types{};

    runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, anon_types);
}

TEST_CASE("Resolve named parameter on select list and where clause with subquery") {
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
    AnonTypeExpects anon_types{};

    runResolveParamType(sql, {schema}, lookup, bound_types, anon_types);
}

TEST_CASE("Resolve duplicated named parameter on select list and where clause with subquery#1 (same type)") {
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
    AnonTypeExpects anon_types{};

    runResolveParamType(sql, {schema}, lookup, bound_types, anon_types);
}

TEST_CASE("Resolve duplicated named parameter on select list and where clause with subquery#2 (NOT same type)") {
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
    AnonTypeExpects anon_types{};

    runResolveParamType(sql, {schema}, lookup, bound_types, anon_types);
}

TEST_CASE("Resolve enum parameter#1 (anonymous/select-list)") {
    std::string sql("select $vis::enum('hide','visible') as vis");

    ParamNameLookup lookup{{"1","vis"}};
    ParamTypeLookup bound_types{{"1","ENUM#1"}};
    AnonTypeExpects anon_types{
        {.kind = UserTypeKind::Enum, .name = "ENUM#1", .fields = { {.field_name = "hide"}, {.field_name = "visible"} }},
    };

    runResolveParamType(sql, {}, lookup, bound_types, anon_types);
}

TEST_CASE("Resolve enum parameter#2 (predefined/select-list)") {
    std::string schema("CREATE TYPE Visibility as ENUM ('hide','visible')");
    std::string sql("select $vis::Visibility as vis");

    ParamNameLookup lookup{{"1","vis"}};
    ParamTypeLookup bound_types{{"1","Visibility"}};
    AnonTypeExpects anon_types{};

    runResolveParamType(sql, {schema}, lookup, bound_types, anon_types);
}

TEST_CASE("Resolve enum parameter#3 (anonymous/where)") {
    std::string schema_1("CREATE TYPE Visibility as ENUM ('hide','visible')");
    std::string schema_2("CREATE TABLE Control (id INTEGER primary key, name VARCHAR not null, vis Visibility not null)");
    std::string sql("select * from Control where vis = $vis::ENUM('hide', 'visible')");

    ParamNameLookup lookup{{"1","vis"}};
    ParamTypeLookup bound_types{{"1","ENUM#1"}};
    AnonTypeExpects anon_types{
        {.kind = UserTypeKind::Enum, .name = "ENUM#1", .fields = { {.field_name = "hide"}, {.field_name = "visible"} }},
    };

    runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, anon_types);
}

TEST_CASE("Resolve enum parameter#3 (predefined/where)") {
    std::string schema_1("CREATE TYPE Visibility as ENUM ('hide','visible')");
    std::string schema_2("CREATE TABLE Control (id INTEGER primary key, name VARCHAR not null, vis Visibility not null)");
    std::string sql("select * from Control where vis = $vis::Visibility");

    ParamNameLookup lookup{{"1","vis"}};
    ParamTypeLookup bound_types{{"1","Visibility"}};
    AnonTypeExpects anon_types{};

    runResolveParamType(sql, {schema_1, schema_2}, lookup, bound_types, anon_types);
}

#endif
