#include <ranges>
#include <cctype>

#include <duckdb.hpp>
#include <duckdb/parser/parser.hpp>

#include "duckdb_params_collector.hpp"
#include "duckdb_binder_support.hpp"

namespace worker {

auto isNumeric(std::string text) -> bool {
    return std::ranges::all_of(text, [](char c){ return std::isdigit(c); });
}

auto evalParameterType(const duckdb::unique_ptr<duckdb::SQLStatement>& stmt) -> ParameterCollector::ParameterType {
    auto view = 
        stmt->named_param_map
         | std::views::keys
         | std::views::filter([](std::string name){ return isNumeric(name); })
    ;
    
    return (bool)view ? ParameterCollector::ParameterType::Positional : ParameterCollector::ParameterType::Named;
}

auto ParameterCollector::ofPosition(std::string old_name) -> std::string {
    if (this->param_type == ParameterCollector::ParameterType::Positional) {
        return old_name;
    }
    else if (this->map.contains(old_name)) {
        return this->map[old_name];
    }
    else {
        ++this->gen_position;
        auto next = std::to_string(*this->gen_position);
        map[old_name] = next;

        return next;
    }
}

}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

using namespace worker;
using namespace Catch::Matchers;

auto runEvalParameterType(const std::string& query, ParameterCollector::ParameterType expected) -> void {
    duckdb::Parser parser;
    parser.ParseQuery(query);

    auto& stmt = parser.statements[0];

    REQUIRE(evalParameterType(stmt) == expected);
}

TEST_CASE("All positional parameter") {
    std::string sql("SELECT $1::int, 123, $2::text FROM Foo where kind = $3");

    runEvalParameterType(sql, ParameterCollector::ParameterType::Positional);
}

TEST_CASE("All named parameter") {
    std::string sql("SELECT $id::int, 123, $name::text FROM Foo where kind = $kind");
    
    runEvalParameterType(sql, ParameterCollector::ParameterType::Named);
}

#endif
