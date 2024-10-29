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
    LogicalParameterVisitor(ParamNameLookup&& names_ref, BoundParamTypeHint&& type_hints_ref, ZmqChannel& channel_ref): 
        names(std::move(names_ref)), type_hints(std::move(type_hints_ref)), channel(channel_ref)
    {
    }
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
    ZmqChannel& channel;
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

        UserTypeKind kind = param_type.id() == duckdb::LogicalTypeId::ENUM ? UserTypeKind::Enum : UserTypeKind::Primitive;

        ParamEntry entry{
            .position = positional,
            .name = this->names.at(positional).name,
            .type_kind = kind,
            .type_name = param_type.ToString(),
            .sort_order = std::stoul(positional)
        };

        return std::pair(positional, entry);
    });

    this->parameters.insert(example_view.begin(), example_view.end());
}

auto LogicalParameterVisitor::VisitReplace(duckdb::BoundParameterExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    UserTypeKind type_kind;
    std::string type_name;

    auto& ret_type = this->type_hints.contains(expr.identifier) ? type_hints.at(expr.identifier)->return_type : expr.return_type;

    if (isEnumUserType(ret_type)) {
        type_name = userTypeName(ret_type);
        if (type_name != "") {
            type_kind = UserTypeKind::User;
            this->user_type_names.push_back(type_name);
        }
        else {
            // process as anonymous user type
            type_kind = UserTypeKind::Enum;
            type_name = std::format("Param::{}#{}", magic_enum::enum_name(UserTypeKind::Enum), expr.identifier);
            this->anon_types.push_back(pickEnumUserType(ret_type, type_name));
        }
    }
    else if (isArrayUserType(ret_type)) {
        this->channel.warn(std::format("[TODO] Notimplemented array/list placeholder type: {} (duckdb-wasm is not supported)", magic_enum::enum_name(ret_type.id())));

        type_kind = UserTypeKind::Primitive;
        type_name = "ANY";
    }
    else if (isStructUserType(ret_type)) {
        this->channel.warn(std::format("Unsupported placeholder type: {} (duckdb-wasm is not supported)", magic_enum::enum_name(ret_type.id())));

        type_kind = UserTypeKind::Primitive;
        type_name = "ANY";
    }
    else {
        type_kind = UserTypeKind::Primitive;
        type_name = ret_type.ToString();
    }
    
    if (! this->parameters.contains(expr.identifier)) {
        this->parameters[expr.identifier] = ParamEntry{
            .position = expr.identifier,
            .name = this->names.at(expr.identifier).name,
            .type_kind = type_kind,
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
    ParamExampleLookup&& examples,
    ZmqChannel& channel) -> ParamResolveResult 
{
    LogicalParameterVisitor visitor(std::move(name_lookup), std::move(type_hints), channel);
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
#include "../resolver.param_type/run.hpp"

using namespace worker;
using namespace Catch::Matchers;

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

auto runResolveParamType(
    const std::string& sql, const std::vector<std::string>& schemas, 
    const ParamNameLookup& expect_name_lookup, const ExpectParamLookup& expect_param_types, 
    UserTypeExpects expect_user_types, AnonTypeExpects expect_anon_types) -> void 
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
    ResolveResult<ParamCollectionResult> walk_result;
    try {
        conn.BeginTransaction();
        walk_result = walkSQLStatement(stmt, ZmqChannel::unitTestChannel());
        bound_result = bindTypeToStatement(*conn.context, std::move(stmts[0]->Copy()), walk_result.data.names, walk_result.data.examples);
        conn.Commit();
    }
    catch (...) {
        conn.Rollback();
        throw;
    }

    auto channel = ZmqChannel::unitTestChannel();
    auto [resolve_result, user_type_names, anon_types] = resolveParamType(
        bound_result.stmt.plan, 
        std::move(walk_result.data.names), 
        std::move(bound_result.type_hints),
        std::move(walk_result.data.examples),
        channel
    );

    parameter_size: {
        INFO("Parameter size");
        REQUIRE(resolve_result.size() == expect_name_lookup.size());
    }
    parameter_entries: {
        for (int i = 0; auto& entry: resolve_result) {
            has_param: {
                INFO(std::format("valid named parameter#{}", i+1));
                REQUIRE(expect_name_lookup.contains(entry.position));
                REQUIRE_THAT(entry.name, Equals(expect_name_lookup.at(entry.position).name));
            }
            type_param_kind: {
                INFO(std::format("valid parameter type kind#{} (has type)", i+1));
                REQUIRE(magic_enum::enum_name(entry.type_kind) == magic_enum::enum_name(expect_param_types.at(entry.position).type_kind));
            }
            if (expect_param_types.contains(entry.position)) {
                with_type_param: {
                    INFO(std::format("valid parameter type#{} (has type)", i+1));
                    REQUIRE(expect_param_types.contains(entry.position));
                    REQUIRE_THAT(entry.type_name.value(), Equals(expect_param_types.at(entry.position).type_name));
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
            REQUIRE(user_type_names.size() == expect_user_types.size());
        }

        std::unordered_set<std::string> lookup(expect_user_types.begin(), expect_user_types.end());

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
            REQUIRE(anon_types.size() == expect_anon_types.size());
        }

        auto view = expect_anon_types | std::views::transform([](auto x) {
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

auto runTransformQuery(std::string& sql, const std::vector<std::string>& schemas, std::string& expect_sql) -> void {
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
    walkSQLStatement(stmt, ZmqChannel::unitTestChannel());

    match_sql: {
        REQUIRE_THAT(stmt->ToString(), Equals(expect_sql));
    }
}

#endif
