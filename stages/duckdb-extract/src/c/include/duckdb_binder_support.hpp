#pragma once

#include <duckdb.hpp>
#include "zmq_worker_support.hpp"
#include "duckdb_nullable_lookup.hpp"

namespace worker {

using PositionalParam = std::string;
using NamedParam = std::string;
using ParamNameLookup = std::unordered_map<PositionalParam, NamedParam>;
using CatalogLookup = std::unordered_map<duckdb::idx_t, duckdb::TableCatalogEntry*>;

enum class StatementParameterStyle {Positional, Named};
enum class StatementType {Invalid, Select};

struct ParamEntry {
    PositionalParam position;
    NamedParam name;
    std::optional<std::string> type_name;
    size_t sort_order;
};

struct ColumnEntry {
    std::string field_name;
    std::string field_type;
    bool nullable;
};

enum class UserTypeKind {Enum};

struct UserTypeEntry {
    struct Member {
        std::string field_name;
        std::optional<std::string> field_type;
    };

    UserTypeKind kind;
    std::string name;
    std::vector<Member> fields;
};

class DummyExpression: public duckdb::Expression {
public:
    DummyExpression(): duckdb::Expression(duckdb::ExpressionType::INVALID, duckdb::ExpressionClass::INVALID, duckdb::LogicalType::SQLNULL) {}
    auto ToString() const -> std::string { return ""; }
    auto Copy() -> duckdb::unique_ptr<Expression> { return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression()); }
};

auto evalParameterType(const duckdb::unique_ptr<duckdb::SQLStatement>& stmt) -> StatementParameterStyle;
auto evalStatementType(const duckdb::unique_ptr<duckdb::SQLStatement>& stmt) -> StatementType;
auto swapMapEntry(std::unordered_map<std::string, std::string> map) -> std::unordered_map<std::string, std::string>;

auto bindTypeToStatement(duckdb::ClientContext& context, duckdb::unique_ptr<duckdb::SQLStatement>&& stmt) -> duckdb::BoundStatement;

auto resolveParamType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, const ParamNameLookup& lookup) -> std::vector<ParamEntry>;
auto resolveColumnType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, StatementType stmt_type, ZmqChannel& channel) -> std::vector<ColumnEntry>;

auto resolveSelectListNullability(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ZmqChannel& channel) -> NullableLookup;

auto resolveUserType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ZmqChannel& channel) -> std::optional<UserTypeEntry>;

}