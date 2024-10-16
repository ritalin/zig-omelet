#pragma once

#include "duckdb_binder_support.hpp"
#include "duckdb_catch2_fmt.hpp"

struct ColumnBindingPair {
    duckdb::ColumnBinding binding;
    worker::ColumnNullableLookup::Item nullable;
};

auto runResolveSelectListNullability(
    const std::string& sql, 
    std::vector<std::string>&& schemas, 
    const std::vector<ColumnBindingPair>& expects) -> void;
