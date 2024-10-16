#pragma once

#include "duckdb_binder_support.hpp"
#include "user_type_support.hpp"

struct ExpectParam {
    worker::UserTypeKind type_kind;
    std::string type_name;
};

using ExpectParamLookup = std::unordered_map<worker::NamedParam, ExpectParam>;
using UserTypeExpects = std::vector<std::string>;
using AnonTypeExpects = std::vector<worker::UserTypeEntry>;

auto runResolveParamType(
    const std::string& sql, const std::vector<std::string>& schemas, 
    const worker::ParamNameLookup& expect_name_lookup, const ExpectParamLookup& expect_param_types, 
    UserTypeExpects expect_user_types, AnonTypeExpects expect_anon_types) -> void;
