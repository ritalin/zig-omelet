#pragma once

#include <duckdb.hpp>

#include "duckdb_params_collector.hpp"

namespace worker {

auto walkSelectStatementInternal(ParameterCollector& collector, duckdb::SelectStatement& stmt, uint32_t depth) -> void;
auto walkTableRef(ParameterCollector& collector, duckdb::unique_ptr<duckdb::TableRef>& table_ref, uint32_t depth) -> void;
auto walkExpression(ParameterCollector& collector, duckdb::unique_ptr<duckdb::ParsedExpression>& expr, uint32_t depth) -> void;

auto walkCTEStatement(ParameterCollector& collector, duckdb::CommonTableExpressionMap& cte) -> void;

}
