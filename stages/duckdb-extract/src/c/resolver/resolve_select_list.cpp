#include <ranges>

#include <duckdb.hpp>
#include <duckdb/planner/logical_operator_visitor.hpp>
#include <duckdb/planner/expression/list.hpp>
#include <duckdb/planner/tableref/list.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>

#include "duckdb_logical_visitors.hpp"
#include "user_type_support.hpp"

#include <iostream>
#include <magic_enum/magic_enum.hpp>

namespace worker {

auto ColumnNameVisitor::VisitReplace(duckdb::BoundConstantExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    this->column_name = expr.value.ToSQLString();
    return nullptr;
}

auto ColumnNameVisitor::Resolve(duckdb::unique_ptr<duckdb::Expression>& expr) -> std::string {
    if (expr->alias != "") {
        return expr->alias;
    }

    ColumnNameVisitor visitor;
    visitor.VisitExpression(&expr);
    
    return std::move(visitor.column_name);
}

static auto evalColumnType(
    const duckdb::LogicalType& ty, 
    std::vector<std::string>& user_type_names, 
    std::vector<UserTypeEntry>& anon_types, 
    std::ranges::iterator_t<AnonymousCounter>& index) -> std::pair<UserTypeKind, std::string> 
{
    std::string type_name = userTypeName(ty);

    if (type_name != "") {
        // Predefined user type
        user_type_names.push_back(type_name);
        return std::make_pair(std::move(UserTypeKind::User), std::move(type_name));
    }

    if (isEnumUserType(ty)) {
        // Anonymous enum type
        type_name = std::format("SelList::Enum#{}", *index++);
        anon_types.push_back(pickEnumUserType(ty, type_name));

        return std::make_pair(std::move(UserTypeKind::Enum), std::move(type_name));
    }

    if (isArrayUserType(ty)) {
        // Anonymous Array type
        type_name = std::format("SelList::Array#{}", *index++);
        anon_types.push_back(pickArrayUserType(ty, type_name, user_type_names, anon_types, index));

        return std::make_pair(std::move(UserTypeKind::Array), std::move(type_name));
    }

    if (isStructUserType(ty)) {
        // Anonymous Struct type
        type_name = std::format("SelList::Struct#{}", *index++);
        anon_types.push_back(pickStructUserType(ty, type_name, user_type_names, anon_types, index));

        return std::make_pair(std::move(UserTypeKind::Struct), std::move(type_name));
    }

    return std::make_pair(std::move(UserTypeKind::Primitive), std::move(ty.ToString()));
}

auto resolveColumnTypeInternal(duckdb::unique_ptr<duckdb::LogicalOperator>& op, const ColumnNullableLookup& join_lookup, ZmqChannel& channel) -> ColumnResolveResult {
    std::vector<ColumnEntry> columns;
    columns.reserve(op->expressions.size());

    std::vector<std::string> user_type_names;
    std::vector<UserTypeEntry> anon_types;

    switch (op->type) {
    case duckdb::LogicalOperatorType::LOGICAL_ORDER_BY:
        {
            if (op->children.size() > 0) {
                return resolveColumnTypeInternal(op->children[0], join_lookup, channel);
            }
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_UNION:
    case duckdb::LogicalOperatorType::LOGICAL_INTERSECT:
    case duckdb::LogicalOperatorType::LOGICAL_EXCEPT:
        // Set-operation Can determin with left-side only
        return resolveColumnTypeInternal(op->children[0], join_lookup, channel);
    case duckdb::LogicalOperatorType::LOGICAL_MATERIALIZED_CTE: 
        return resolveColumnTypeInternal(op->children[1], join_lookup, channel);
    case duckdb::LogicalOperatorType::LOGICAL_PROJECTION: 
        {
            auto& op_projection = op->Cast<duckdb::LogicalProjection>();
            auto index = AnonymousCounter(1).begin();
            std::unordered_multiset<std::string> name_dupe{};

            for (size_t i = 0; auto& expr: op->expressions) {
                auto [type_kind, type_name] = evalColumnType(expr->return_type, user_type_names, anon_types, index);

                columns: {
                    ColumnNullableLookup::Column binding{
                        .table_index = op_projection.table_index, 
                        .column_index = i++,
                    };
                    auto field_name = ColumnNameVisitor::Resolve(expr);

                    auto dupe_count = name_dupe.count(field_name);
                    name_dupe.insert(field_name);

                    auto entry = ColumnEntry{
                        .field_name = dupe_count == 0 ? field_name : std::format("{}_{}", field_name, dupe_count),
                        .type_kind = type_kind,
                        .field_type = type_name,
                        .nullable = join_lookup[binding].shouldNulls(),
                    };

                    columns.emplace_back(std::move(entry));
                }
            }
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_DELETE:
    case duckdb::LogicalOperatorType::LOGICAL_UPDATE:
    case duckdb::LogicalOperatorType::LOGICAL_INSERT:
        // Top-level delete/update does not have returning field(s)
        break;
    default:
        channel.warn(std::format("[TODO] Can not resolve column type: {}", magic_enum::enum_name(op->type)));
        break;
    }

    return {
        .columns = std::move(columns),
        .user_type_names = std::move(user_type_names),
        .anon_types = std::move(anon_types),
    };
}

const std::unordered_set<StatementType> supprted_stmts{
    StatementType::Select,
    StatementType::Delete,
    StatementType::Update,
    StatementType::Insert,
};

auto resolveColumnType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, StatementType stmt_type, duckdb::Connection& conn, ZmqChannel& channel) -> ColumnResolveResult {
    if (!supprted_stmts.contains(stmt_type)) return {};

    auto [join_types, _] = resolveSelectListNullability(op, conn, channel);

    return resolveColumnTypeInternal(op, join_types, channel);
}

}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <utility>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/matchers/catch_matchers_vector.hpp>

#include "duckdb_database.hpp"
#include "../resolver.select_list/run.hpp"

using namespace worker;
using namespace Catch::Matchers;

static auto expectAnonymousUserType(UserTypeEntry actual, UserTypeEntry expect, size_t i, size_t depth) -> void {
    INFO(std::format("[{}] Anonymous type#{}", depth, i));
    user_type_kind: {
        UNSCOPED_INFO("user type kind");
        CHECK(magic_enum::enum_name(actual.kind) == magic_enum::enum_name(expect.kind));
    }
    user_type_name: {
        UNSCOPED_INFO("user type name");
        CHECK_THAT(actual.name, Equals(expect.name));
    }
    user_type_field_size: {
        UNSCOPED_INFO("user type field size");
        REQUIRE(actual.fields.size() == expect.fields.size());
    }
    user_type_fields: {
        for (int j = 0; auto& field: actual.fields) {
            INFO(std::format("type field#{} of Anonymous type#{}", j, i+1));
            field_name: {
                UNSCOPED_INFO("user type field name");
                CHECK_THAT(field.field_name, Equals(expect.fields[j].field_name));
            }
            field_type: {
                UNSCOPED_INFO("has user type field type");
                REQUIRE((bool)field.field_type == (bool)expect.fields[j].field_type);

                if (field.field_type) {
                    expectAnonymousUserType(*field.field_type, *expect.fields[j].field_type, j, depth+1);
                }
            }
            ++j;
        }
    }
}

auto runBindStatement(
    const std::string sql, const std::vector<std::string>& schemas, 
    const std::vector<ColumnEntry>& expects, 
    const std::vector<std::string>& expect_user_types,
    const std::vector<UserTypeEntry>& expect_anon_types) -> void 
{
    auto db = Database();
    auto conn = db.connect();

    prepare_schema: {
        for (auto& schema: schemas) {
            conn.Query(schema);
        }
        db.retainUserTypeName(conn);
    }

    ColumnResolveResult column_result;
    try {
        conn.BeginTransaction();

        auto channel = ZmqChannel::unitTestChannel();
        
        auto stmts = conn.ExtractStatements(sql);
        auto stmt_type = evalStatementType(stmts[0]);
        auto walk_result = walkSQLStatement(stmts[0], ZmqChannel::unitTestChannel());
        auto bound_result = bindTypeToStatement(*conn.context, std::move(stmts[0]->Copy()), walk_result.names, {});
        column_result = resolveColumnType(bound_result.stmt.plan, stmt_type, conn, channel);
        conn.Commit();
    }
    catch (...) {
        conn.Rollback();
        throw;
    }

    result_size: {
        INFO("Result size");
        REQUIRE(column_result.columns.size() == expects.size());
    }
    result_entries: {
        for (int i = 0; auto& entry: column_result.columns) {
            INFO(std::format("entry#{} (`{}`)", i+1, expects[i].field_name));
            field_name: {
                INFO("field name");
                CHECK_THAT(entry.field_name, Equals(expects[i].field_name));
            }
            field_category: {
                INFO("field type kind");
                CHECK(magic_enum::enum_name(entry.type_kind) == magic_enum::enum_name(expects[i].type_kind));
            }
            field_type: {
                INFO("field type");
                CHECK_THAT(entry.field_type, Equals(expects[i].field_type));
            }
            nullable: {
                INFO("nullable");
                CHECK(entry.nullable == expects[i].nullable);
            }
            ++i;
        }
    }
    user_types_size: {
        INFO("user types size");
        REQUIRE(column_result.user_type_names.size() == expect_user_types.size());
    }
    user_types: {
        for (auto& expect_name: expect_user_types) {
            INFO(std::format("has user type name (`{}`)", expect_name));
            CHECK_THAT(column_result.user_type_names, VectorContains(expect_name));
        }
    }
    anonymous_types_size: {
        INFO("Anonymous types size");
        REQUIRE(column_result.anon_types.size() == expect_anon_types.size());
    }
    anonymous_types: {
        auto anon_type_view = column_result.anon_types | std::views::transform([](const auto& x) {
            return std::pair<std::string, UserTypeEntry>(x.name, x);
        });
        std::unordered_map<std::string, UserTypeEntry> anon_type_lookup(anon_type_view.begin(), anon_type_view.end());

        for (int i = 0; auto& anon: expect_anon_types) {
            INFO(std::format("is exist field identifier#{} ({})", i+1, anon.name));
            REQUIRE(anon_type_lookup.contains(anon.name));

            INFO(std::format("check field recursively#{}", anon.name));
            expectAnonymousUserType(anon_type_lookup.at(anon.name), anon, i, 1);
            ++i;
        }
    }
}

#endif