#pragma once

#include <duckdb.hpp>
#include "zmq_worker_support.hpp"

namespace worker {

using PositionalParam = std::string;
using NamedParam = std::string;
using ParamNameLookup = std::unordered_map<PositionalParam, NamedParam>;

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

auto evalParameterType(const duckdb::unique_ptr<duckdb::SQLStatement>& stmt) -> StatementParameterStyle;
auto evalStatementType(const duckdb::unique_ptr<duckdb::SQLStatement>& stmt) -> StatementType;
auto swapMapEntry(std::unordered_map<std::string, std::string> map) -> std::unordered_map<std::string, std::string>;

auto bindTypeToTableRef(duckdb::ClientContext& context, duckdb::unique_ptr<duckdb::SQLStatement>&& stmt, StatementType type) -> duckdb::unique_ptr<duckdb::BoundTableRef>;
auto bindTypeToStatement(duckdb::ClientContext& context, duckdb::unique_ptr<duckdb::SQLStatement>&& stmt) -> duckdb::BoundStatement;

auto resolveParamType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, const ParamNameLookup& lookup) -> std::vector<ParamEntry>;
auto resolveColumnType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, duckdb::unique_ptr<duckdb::BoundTableRef>&& table_ref, StatementType stmt_type) -> std::vector<ColumnEntry>;

}