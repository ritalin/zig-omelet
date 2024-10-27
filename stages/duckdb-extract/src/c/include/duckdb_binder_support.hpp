#pragma once

#include <duckdb.hpp>
#include <duckdb/parser/expression/cast_expression.hpp>

#include "zmq_worker_support.hpp"
#include "duckdb_nullable_lookup.hpp"
#include "user_type_support.hpp"

namespace worker {

struct ParamLookupEntry;
struct ParamExample;

using PositionalParam = std::string;
using NamedParam = std::string;
using ParamNameLookup = std::unordered_map<PositionalParam, ParamLookupEntry>;
using ParamExampleLookup = std::unordered_map<PositionalParam, ParamExample>;

using CatalogLookup = std::unordered_map<duckdb::idx_t, duckdb::TableCatalogEntry*>;
using BoundParamTypeHint = std::unordered_map<PositionalParam, duckdb::unique_ptr<duckdb::Expression>>;

enum class ResolverStatus {Unhandled, Handled};

struct Nullability {
    bool from_field;
    bool from_join;
public:
    auto shouldNulls() -> bool { return this->from_join || this->from_field; }
};

using ColumnNullableLookup = GenericNullableLookup<Nullability>;
struct ColumnNullabilityResult {
    ColumnNullableLookup nullabilities;
    ResolverStatus status;
};

struct SampleNullabilityNode {
    std::string name;
    Nullability nullable;
    std::vector<std::shared_ptr<SampleNullabilityNode>> children;
public:
    auto findByName(const std::string& name) -> std::shared_ptr<SampleNullabilityNode>;
};

using SampleNullableCache = GenericNullableLookup<std::shared_ptr<SampleNullabilityNode>>;

enum class StatementParameterStyle {Positional, Named};
enum class StatementType {Invalid, Select, Insert, Delete, Update};
enum class ExampleKind {General, Path};

struct ParamLookupEntry {
    std::string name;
    std::shared_ptr<duckdb::ParsedExpression> type_hint = nullptr;
};
struct ParamExample {
    ExampleKind kind;
    duckdb::Value value;
};

struct ParamCollectionResult {
    StatementType type;
    ParamNameLookup names;
    ParamExampleLookup examples;
};
struct BoundResult {
    duckdb::BoundStatement stmt;
    BoundParamTypeHint type_hints;
};

struct ParamEntry {
    PositionalParam position;
    NamedParam name;
    UserTypeKind type_kind;
    std::optional<std::string> type_name;
    size_t sort_order;
};

struct ColumnEntry {
    std::string field_name;
    UserTypeKind type_kind;
    std::string field_type;
    bool nullable;
};

struct ParamResolveResult {
    std::vector<ParamEntry> params;
    std::vector<std::string> user_type_names;
    std::vector<UserTypeEntry> anon_types;
};
struct ColumnResolveResult {
    std::vector<ColumnEntry> columns;
    std::vector<std::string> user_type_names;
    std::vector<UserTypeEntry> anon_types;
};

struct UserTypeResult {
    UserTypeEntry entry;
    std::vector<std::string> user_type_names;
    std::vector<UserTypeEntry> anon_types;
};

enum class ResolveStatus {Unhandled, Handled};

template<typename ResultData>
struct ResolveResult {
    ResultData data;
    ResolveStatus handled;
};

class DummyExpression: public duckdb::Expression {
public:
    DummyExpression(): duckdb::Expression(duckdb::ExpressionType::INVALID, duckdb::ExpressionClass::INVALID, duckdb::LogicalType::SQLNULL) {}
    auto ToString() const -> std::string { return ""; }
    auto Copy() const -> duckdb::unique_ptr<Expression> { return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression()); }
};

auto evalParameterType(const duckdb::unique_ptr<duckdb::SQLStatement>& stmt) -> StatementParameterStyle;
auto evalStatementType(const duckdb::unique_ptr<duckdb::SQLStatement>& stmt) -> StatementType;
auto swapMapEntry(const std::unordered_map<std::string, ParamLookupEntry>& map) -> std::unordered_map<std::string, ParamLookupEntry>;
auto walkSQLStatement(duckdb::unique_ptr<duckdb::SQLStatement>& stmt, ZmqChannel&& channel) -> ResolveResult<ParamCollectionResult>;

auto bindTypeToStatement(duckdb::ClientContext& context, duckdb::unique_ptr<duckdb::SQLStatement>&& stmt, const ParamNameLookup& names, const ParamExampleLookup& examples) -> BoundResult;

auto resolveParamType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ParamNameLookup&& name_lookup, BoundParamTypeHint&& type_hints, ParamExampleLookup&& examples, ZmqChannel& channel) -> ParamResolveResult;
auto resolveColumnType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, StatementType stmt_type, duckdb::Connection& conn, ZmqChannel& channel) -> ColumnResolveResult;
auto resolveSelectListNullability(duckdb::unique_ptr<duckdb::LogicalOperator>& op, duckdb::Connection& conn, ZmqChannel& channel) -> ColumnNullabilityResult;
auto resolveUserType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ZmqChannel& channel) -> std::optional<UserTypeResult>;

}