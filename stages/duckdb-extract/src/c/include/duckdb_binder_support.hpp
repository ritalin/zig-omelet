#pragma once

#include <duckdb.hpp>
#include "zmq_worker_support.hpp"
#include "duckdb_params_collector.hpp"

namespace worker {

typedef std::string PositionalParam;
typedef std::string NamedParam;
typedef std::unordered_map<PositionalParam, NamedParam> ParamNameLookup;

enum class StatementParameterStyle {Positional, Named};

struct ParamEntry {
    PositionalParam position;
    NamedParam name;
    std::optional<std::string> type_name;
    size_t sort_order;
};

auto evalParameterType(const duckdb::unique_ptr<duckdb::SQLStatement>& stmt) -> StatementParameterStyle;
auto swapMapEntry(std::unordered_map<std::string, std::string> map) -> std::unordered_map<std::string, std::string>;

auto resolveParamType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, const ParamNameLookup& lookup) -> std::vector<ParamEntry>;
auto resolveColumnType(duckdb::ClientContext context, duckdb::SQLStatement& stmt) -> duckdb::unique_ptr<duckdb::BoundTableRef>;

}