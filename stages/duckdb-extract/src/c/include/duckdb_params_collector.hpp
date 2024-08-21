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
public:
    auto ofPosition(std::string old_name) -> std::string;
    auto paramUserType(std::string position, std::string type_name) -> void;
private:
    StatementParameterStyle param_type;
    std::ranges::iterator_t<std::ranges::iota_view<size_t>> gen_position;
    std::unordered_map<NamedParam, PositionalParam> name_map;
    std::unordered_map<PositionalParam, std::string> param_user_type_map;
    std::unordered_map<duckdb::idx_t, std::string> sel_list_user_type_map;
};

}