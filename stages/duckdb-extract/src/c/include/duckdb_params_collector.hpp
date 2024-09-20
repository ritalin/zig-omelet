#pragma once

#include <ranges>

#include <duckdb.hpp>

#include "duckdb_binder_support.hpp"
#include "zmq_worker_support.hpp"

namespace worker {

class ParameterCollector {
public:
    using Result = ParamCollectionResult;
public:
    ZmqChannel channel;
public:
    ParameterCollector(StatementParameterStyle param_type, ZmqChannel&& channel): 
        param_type(param_type), gen_position(std::ranges::begin(std::ranges::iota_view<size_t>{0})), channel(channel) 
    {
    }
public:
    auto walkSelectStatement(duckdb::SelectStatement& stmt) -> Result;
    auto walkCTEStatement(duckdb::CommonTableExpressionMap& cte) -> Result;
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