#pragma once

#include <ranges>

#include <duckdb.hpp>

#include "cbor_encode.hpp"

namespace worker {

enum class UserTypeKind {Enum, Struct, Array, Primitive, Alias, User};

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

using AnonymousCounter = std::ranges::iota_view<size_t>;

auto userTypeName(const duckdb::LogicalType& ty) -> std::string;

auto isEnumUserType(const duckdb::LogicalType &ty) -> bool;
auto pickEnumUserType(const duckdb::LogicalType &ty, const std::string& type_name) -> UserTypeEntry;

auto isStructUserType(const duckdb::LogicalType &ty) -> bool;
auto pickStructUserType(const duckdb::LogicalType& ty, const std::string& type_name, std::vector<std::string>& nested_user_types, std::vector<UserTypeEntry>& nested_anon_types, std::ranges::iterator_t<AnonymousCounter>& index) -> UserTypeEntry;

auto isArrayUserType(const duckdb::LogicalType &ty) -> bool;
auto pickArrayUserType(const duckdb::LogicalType &ty, const std::string& type_name, std::vector<std::string>& user_type_names, std::vector<UserTypeEntry>& anon_types, std::ranges::iterator_t<AnonymousCounter>& index) -> UserTypeEntry;

auto isAliasUserType(const duckdb::LogicalType &ty) -> bool;
auto pickAliasUserType(const duckdb::LogicalType &ty, const std::string& type_name, std::vector<std::string>& user_type_names, std::ranges::iterator_t<AnonymousCounter>& index) -> UserTypeEntry;

auto encodeUserType(CborEncoder& encoder, const UserTypeEntry& entry) -> void;

}
