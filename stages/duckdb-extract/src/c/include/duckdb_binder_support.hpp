#pragma once

#include <duckdb.hpp>
#include "zmq_worker_support.hpp"
#include "duckdb_nullable_lookup.hpp"
#include "user_type_support.hpp"

namespace worker {

using PositionalParam = std::string;
using NamedParam = std::string;
using ParamNameLookup = std::unordered_map<PositionalParam, NamedParam>;

using CatalogLookup = std::unordered_map<duckdb::idx_t, duckdb::TableCatalogEntry*>;

enum class StatementParameterStyle {Positional, Named};
enum class StatementType {Invalid, Select};

struct ParamCollectionResult {
    StatementType type;
    ParamNameLookup names;
};
struct ParamEntry {
    PositionalParam position;
    NamedParam name;
    std::optional<std::string> type_name;
    size_t sort_order;
};

struct CteColumnEntry {
    std::string name;
    NullableLookup::Column binding;
};
using CteColumnBindings = std::unordered_map<duckdb::idx_t, std::vector<CteColumnEntry>>;
using CteColumnBindingsRef = std::reference_wrapper<const CteColumnBindings>;

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
auto swapMapEntry(std::unordered_map<std::string, std::string> map) -> std::unordered_map<std::string, std::string>;
auto walkSQLStatement(duckdb::unique_ptr<duckdb::SQLStatement>& stmt, ZmqChannel&& channel) -> ParamCollectionResult;

auto bindTypeToStatement(duckdb::ClientContext& context, duckdb::unique_ptr<duckdb::SQLStatement>&& stmt) -> duckdb::BoundStatement;

auto resolveParamType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ParamNameLookup&& name_lookup) -> ParamResolveResult;
auto resolveColumnType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, StatementType stmt_type, ZmqChannel& channel) -> ColumnResolveResult;
auto resolveSelectListNullability(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ZmqChannel& channel) -> NullableLookup;
auto resolveUserType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ZmqChannel& channel) -> std::optional<UserTypeEntry>;

}