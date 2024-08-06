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

struct ColumnBindingPair {
    duckdb::ColumnBinding binding;
    bool nullable;
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

auto bindTypeToTableRef(duckdb::ClientContext& context, duckdb::unique_ptr<duckdb::SQLStatement>&& stmt, StatementType type) -> duckdb::unique_ptr<duckdb::BoundTableRef>;
auto bindTypeToStatement(duckdb::ClientContext& context, duckdb::unique_ptr<duckdb::SQLStatement>&& stmt) -> duckdb::BoundStatement;

auto resolveParamType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, const ParamNameLookup& lookup) -> std::vector<ParamEntry>;
auto resolveColumnType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, duckdb::unique_ptr<duckdb::BoundTableRef>& table_ref, StatementType stmt_type) -> std::vector<ColumnEntry>;

auto createColumnBindingLookup(duckdb::unique_ptr<duckdb::LogicalOperator>& op, duckdb::unique_ptr<duckdb::BoundTableRef>& table_ref, const CatalogLookup& catalogs) -> std::vector<ColumnBindingPair>;
auto createJoinTypeLookup(duckdb::unique_ptr<duckdb::LogicalOperator>& op, const CatalogLookup& catalogs) -> NullableLookup;

}