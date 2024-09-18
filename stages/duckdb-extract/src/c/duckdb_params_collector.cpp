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

auto evalParameterType(const duckdb::unique_ptr<duckdb::SQLStatement>& stmt) -> StatementParameterStyle {
    auto view = 
        stmt->named_param_map
         | std::views::keys
         | std::views::filter([](std::string name){ return isNumeric(name); })
    ;
    
    return (bool)view ? StatementParameterStyle::Positional : StatementParameterStyle::Named;
}

auto evalStatementType(const duckdb::unique_ptr<duckdb::SQLStatement>& stmt) -> StatementType {
    switch (stmt->type) {
    case duckdb::StatementType::SELECT_STATEMENT: return StatementType::Select;
    case duckdb::StatementType::INSERT_STATEMENT: return StatementType::Invalid; // TODO: Need supports
    case duckdb::StatementType::UPDATE_STATEMENT: return StatementType::Invalid; // TODO: Need supports
    case duckdb::StatementType::DELETE_STATEMENT: return StatementType::Invalid; // TODO: Need supports
    default: return StatementType::Invalid;
    }
}

auto swapMapEntry(const std::unordered_map<std::string, ParamLookupEntry>& map) -> std::unordered_map<std::string, ParamLookupEntry> {
    auto swap_entries = map | std::views::transform([](auto& pair) {  
        auto& [key, value] = pair;
        return std::pair<std::string, ParamLookupEntry>{value.name, ParamLookupEntry(key, value.type_hint) };
    });

    return std::unordered_map<std::string, ParamLookupEntry>(swap_entries.begin(), swap_entries.end());
}

auto ParameterCollector::ofPosition(std::string old_name) -> std::string {
    if (this->name_map.contains(old_name)) {
        return this->name_map.at(old_name).name;
    }    
    else if (this->param_type == StatementParameterStyle::Positional) {
        this->name_map.emplace(old_name, ParamLookupEntry(old_name));
        return old_name;
    }
    else {
        ++this->gen_position;
        auto next = std::to_string(*this->gen_position);
        this->name_map.emplace(old_name, ParamLookupEntry(next));

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

auto runEvalParameterType(const std::string& query, StatementParameterStyle expected) -> void {
    duckdb::Parser parser;
    parser.ParseQuery(query);

    auto& stmt = parser.statements[0];

    REQUIRE(evalParameterType(stmt) == expected);
}

TEST_CASE("All positional parameter") {
    std::string sql("SELECT $1::int, 123, $2::text FROM Foo where kind = $3");

    runEvalParameterType(sql, StatementParameterStyle::Positional);
}

TEST_CASE("All named parameter") {
    std::string sql("SELECT $id::int, 123, $name::text FROM Foo where kind = $kind");
    
    runEvalParameterType(sql, StatementParameterStyle::Named);
}

#endif
