#pragma once

#include <duckdb.hpp>
#include "zmq_worker_support.hpp"
#include "duckdb_nullable_lookup.hpp"
#include "cbor_encode.hpp"

namespace worker {

using PositionalParam = std::string;
using NamedParam = std::string;
using ParamNameLookup = std::unordered_map<PositionalParam, NamedParam>;

template<typename TKey>
using UserTypeLookup = std::unordered_map<TKey, std::string>;

using CatalogLookup = std::unordered_map<duckdb::idx_t, duckdb::TableCatalogEntry*>;

enum class StatementParameterStyle {Positional, Named};
enum class StatementType {Invalid, Select};

struct ParamCollectionResult {
    StatementType type;
    ParamNameLookup names;
    UserTypeLookup<PositionalParam> param_user_types;
    UserTypeLookup<duckdb::idx_t> sel_list_user_types;
};
struct ParamEntry {
    PositionalParam position;
    NamedParam name;
    std::optional<std::string> type_name;
    size_t sort_order;
};

struct ColumnEntry {
    std::string field_name;
    std::string field_type;
    bool nullable;
};

enum class UserTypeKind {Enum};

struct UserTypeEntry {
    struct Member {
        std::string field_name;
        std::optional<std::string> field_type;
    };

    UserTypeKind kind;
    std::string name;
    std::vector<Member> fields;
};

struct ParamResolveResult {
    std::vector<ParamEntry> params;
    std::vector<UserTypeEntry> anon_types;
};

class DummyExpression: public duckdb::Expression {
public:
    DummyExpression(): duckdb::Expression(duckdb::ExpressionType::INVALID, duckdb::ExpressionClass::INVALID, duckdb::LogicalType::SQLNULL) {}
    auto ToString() const -> std::string { return ""; }
    auto Copy() -> duckdb::unique_ptr<Expression> { return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression()); }
};

auto evalParameterType(const duckdb::unique_ptr<duckdb::SQLStatement>& stmt) -> StatementParameterStyle;
auto evalStatementType(const duckdb::unique_ptr<duckdb::SQLStatement>& stmt) -> StatementType;
auto swapMapEntry(std::unordered_map<std::string, std::string> map) -> std::unordered_map<std::string, std::string>;
auto walkSQLStatement(duckdb::unique_ptr<duckdb::SQLStatement>& stmt, ZmqChannel&& channel) -> ParamCollectionResult;

auto bindTypeToStatement(duckdb::ClientContext& context, duckdb::unique_ptr<duckdb::SQLStatement>&& stmt) -> duckdb::BoundStatement;

auto resolveParamType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ParamNameLookup&& name_lookup, const UserTypeLookup<PositionalParam>& user_type_lookup) -> ParamResolveResult;
auto resolveColumnType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, StatementType stmt_type, ZmqChannel& channel) -> std::vector<ColumnEntry>;
auto resolveSelectListNullability(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ZmqChannel& channel) -> NullableLookup;
auto resolveUserType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ZmqChannel& channel) -> std::optional<UserTypeEntry>;

auto pickEnumUserType(const duckdb::LogicalType &ty, const std::string& type_name) -> UserTypeEntry;
auto encodeUserType(CborEncoder& encoder, const UserTypeEntry& entry) -> void;

}