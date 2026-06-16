#pragma once

#include <ranges>

#include <duckdb.hpp>
#include <duckdb/parser/statement/list.hpp>

#include "duckdb_binder_support.hpp"
#include "zmq_worker_support.hpp"

namespace worker {

class ParameterCollector {
public:
    using Result = ParamCollectionResult;
public:
    NngChannel& channel;
public:
    ParameterCollector(StatementParameterStyle param_type, NngChannel& channel): 
        channel(channel), param_type(param_type), gen_position(std::ranges::begin(std::ranges::iota_view<size_t>{0}))
    {
    }
public:
    auto walkSelectStatement(duckdb::SelectStatement& stmt) -> Result;
    auto walkDeleteStatement(duckdb::DeleteStatement& stmt) -> Result;
    auto walkUpdateStatement(duckdb::UpdateStatement& stmt) -> Result;
    auto walkInsertStatement(duckdb::InsertStatement& stmt) -> Result;
public:
    auto ofPosition(std::string old_name) -> std::string;
    auto attachTypeHint(PositionalParam name, std::unique_ptr<duckdb::ParsedExpression>&& type_hint) -> void;
    auto putSampleValue(PositionalParam name, ExampleKind kind, const duckdb::Value & sample_value) -> void;
private:
    StatementParameterStyle param_type;
    std::ranges::iterator_t<std::ranges::iota_view<size_t>> gen_position;
    std::unordered_map<NamedParam, ParamLookupEntry> name_map;
    ParamExampleLookup examples;
};

}