#pragma once

#include <duckdb.hpp>

#include "cbor_encode.hpp"

namespace worker {

enum class UserTypeKind {Enum};

struct UserTypeEntry;

struct UserTypeMember {
public:
    std::string field_name;
    std::shared_ptr<UserTypeEntry> field_type;
public:
    UserTypeMember(const std::string& name, std::shared_ptr<UserTypeEntry> type = nullptr): field_name(name), field_type(type) {}
};

struct UserTypeEntry {
    using Member = struct UserTypeMember;
public:
    UserTypeKind kind;
    std::string name;
    std::vector<Member> fields;
};

auto userTypeName(const duckdb::LogicalType& ty) -> std::string;

auto isEnumUserType(const duckdb::LogicalType &ty) -> bool;
auto pickEnumUserType(const duckdb::LogicalType &ty, const std::string& type_name) -> UserTypeEntry;
auto encodeUserType(CborEncoder& encoder, const UserTypeEntry& entry) -> void;

}
