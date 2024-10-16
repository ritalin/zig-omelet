#pragma once

#include "duckdb_binder_support.hpp"
#include "duckdb_catch2_fmt.hpp"

using LogicalOperatorRef = duckdb::unique_ptr<duckdb::LogicalOperator>;
using BoundTableRef = duckdb::unique_ptr<duckdb::BoundTableRef>;

auto runBindStatement(
    const std::string sql, const std::vector<std::string>& schemas, 
    const std::vector<worker::ColumnEntry>& expects, 
    const std::vector<std::string>& expect_user_types,
    const std::vector<worker::UserTypeEntry>& expect_anon_types) -> void;
