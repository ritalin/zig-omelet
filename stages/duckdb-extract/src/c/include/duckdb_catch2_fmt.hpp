#pragma once

#include <duckdb/planner/column_binding.hpp>

#include <catch2/catch_all.hpp>

#include "duckdb_nullable_lookup.hpp"

namespace worker {
    inline std::ostream& operator << (std::ostream& os, ColumnNullableLookup::Column const& v) {
        os << std::format("({}, {})", v.table_index, v.column_index);
        return os;
    }
}
