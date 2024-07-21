#pragma once

#include <ranges>

#include <duckdb.hpp>

#include "duckdb_binder_support.hpp"
#include "zmq_worker_support.hpp"

namespace worker {

class ParameterCollector {
public:
    enum class StatementType {Invalid, Select};
    struct Result {
        StatementType type;
        ParamNameLookup lookup;
    };
public:
    ZmqChannel channel;
public:
    ParameterCollector(StatementParameterStyle param_type, ZmqChannel&& channel): 
        param_type(param_type), gen_position(std::ranges::begin(std::ranges::iota_view<size_t>{0})), channel(channel) 
    {
    }
public:
    auto walkSelectStatement(duckdb::SelectStatement& stmt) -> Result;
public:
    auto ofPosition(std::string old_name) -> std::string;
private:
    StatementParameterStyle param_type;
    std::ranges::iterator_t<std::ranges::iota_view<size_t>> gen_position;
    std::unordered_map<NamedParam, PositionalParam> map;
};

}