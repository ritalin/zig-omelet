#pragma once

#include <duckdb.hpp>
#include <duckdb/parser/expression/cast_expression.hpp>

#include "zmq_worker_support.hpp"
#include "duckdb_nullable_lookup.hpp"
#include "user_type_support.hpp"

namespace worker {

struct ParamLookupEntry;

using PositionalParam = std::string;
using NamedParam = std::string;
using ParamNameLookup = std::unordered_map<PositionalParam, ParamLookupEntry>;

using CatalogLookup = std::unordered_map<duckdb::idx_t, duckdb::TableCatalogEntry*>;
using BoundParamTypeHint = std::unordered_map<PositionalParam, duckdb::unique_ptr<duckdb::Expression>>;

enum class StatementParameterStyle {Positional, Named};
enum class StatementType {Invalid, Select};

struct ParamLookupEntry {
    std::string name;
    std::shared_ptr<duckdb::ParsedExpression> type_hint = nullptr;
};

struct ParamCollectionResult {
    StatementType type;
    ParamNameLookup names;
};
struct BoundResult {
    duckdb::BoundStatement stmt;
    BoundParamTypeHint type_hints;
};

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

struct ParamResolveResult {
    std::vector<ParamEntry> params;
    std::vector<std::string> user_type_names;
    std::vector<UserTypeEntry> anon_types;
};
struct ColumnResolveResult {
    std::vector<ColumnEntry> columns;
    std::vector<std::string> user_type_names;
    std::vector<UserTypeEntry> anon_types;
};

class DummyExpression: public duckdb::Expression {
public:
    DummyExpression(): duckdb::Expression(duckdb::ExpressionType::INVALID, duckdb::ExpressionClass::INVALID, duckdb::LogicalType::SQLNULL) {}
    auto ToString() const -> std::string { return ""; }
    auto Copy() const -> duckdb::unique_ptr<Expression> { return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression()); }
};

auto evalParameterType(const duckdb::unique_ptr<duckdb::SQLStatement>& stmt) -> StatementParameterStyle;
auto evalStatementType(const duckdb::unique_ptr<duckdb::SQLStatement>& stmt) -> StatementType;
auto swapMapEntry(const std::unordered_map<std::string, ParamLookupEntry>& map) -> std::unordered_map<std::string, ParamLookupEntry>;
auto walkSQLStatement(duckdb::unique_ptr<duckdb::SQLStatement>& stmt, ZmqChannel&& channel) -> ParamCollectionResult;

auto bindTypeToStatement(duckdb::ClientContext& context, duckdb::unique_ptr<duckdb::SQLStatement>&& stmt, const ParamNameLookup& names) -> BoundResult;

auto resolveParamType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ParamNameLookup&& name_lookup, BoundParamTypeHint&& type_hints) -> ParamResolveResult;
auto resolveColumnType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, StatementType stmt_type, ZmqChannel& channel) -> ColumnResolveResult;
auto resolveSelectListNullability(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ZmqChannel& channel) -> NullableLookup;
auto resolveUserType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ZmqChannel& channel) -> std::optional<UserTypeEntry>;

}