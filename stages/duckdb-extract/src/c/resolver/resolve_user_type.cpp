#include <duckdb.hpp>
#include <duckdb/planner/operator/logical_create.hpp>
#include <duckdb/planner/operator/logical_create_table.hpp>
#include <duckdb/planner/operator/logical_create_index.hpp>
#include <duckdb/parser/parsed_data/create_type_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parsed_data/create_sequence_info.hpp>
#include <duckdb/common/extra_type_info.hpp>

#include <magic_enum/magic_enum.hpp>

#include "duckdb_binder_support.hpp"

namespace worker {

class CreateOperatorVisitor: public duckdb::LogicalOperatorVisitor {
public:
    bool handled = false;
public:
    CreateOperatorVisitor(UserTypeEntry& entry_ref, ZmqChannel& channel_ref): channel(channel_ref), entry(entry_ref) {}
public:
    auto VisitOperator(duckdb::unique_ptr<duckdb::LogicalOperator>& op) -> void;
private:
    ZmqChannel& channel;
    UserTypeEntry& entry;
};

static auto resolveEnumType(UserTypeEntry& entry, duckdb::CreateTypeInfo& info) -> void {
    entry.kind = UserTypeKind::Enum;
    entry.name = info.name;

    auto& ext_info = info.type.AuxInfo()->Cast<duckdb::EnumTypeInfo>();
    auto members = duckdb::FlatVector::GetData<duckdb::string_t>(ext_info.GetValuesInsertOrder());
    auto member_size = ext_info.GetDictSize();

    entry.fields.reserve(member_size);

    for (const auto *it = members; it != members + member_size; ++it) {
        entry.fields.emplace_back(it->GetString());
    }
}

auto CreateOperatorVisitor::VisitOperator(duckdb::unique_ptr<duckdb::LogicalOperator>& op) -> void {
    switch (op->type) {
    case duckdb::LogicalOperatorType::LOGICAL_CREATE_TYPE: 
        {
            auto& op_create = op->Cast<duckdb::LogicalCreate>();
            auto& info = op_create.info->Cast<duckdb::CreateTypeInfo>();
            if (info.type.Contains(duckdb::LogicalTypeId::ENUM)) {
                this->handled = true;
                resolveEnumType(this->entry, info);
            }
            else {
                this->channel.warn(std::format("[TODO] Unsupported user defined type: {}", magic_enum::enum_name(info.type.id())));
            }
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_CREATE_TABLE:
        {
            auto& op_create = op->Cast<duckdb::LogicalCreateTable>();
            auto& info = op_create.info->Base();
            this->channel.info(std::format("Skip for no user defined type: Table/{}.{}", info.schema, info.table));
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_CREATE_INDEX:
        {
            auto& op_create = op->Cast<duckdb::LogicalCreateIndex>();
            auto& info = op_create.info;
            this->channel.info(std::format("Skip for no user defined type: Index/{}.{}", info->schema, info->index_name));
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_CREATE_SEQUENCE:
        {
            auto& op_create = op->Cast<duckdb::LogicalCreate>();
            auto& info = op_create.info->Cast<duckdb::CreateSequenceInfo>();
            this->channel.info(std::format("Skip for no user defined type: Sequence/{}.{}", info.schema, info.name));
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_CREATE_VIEW:
        {
            auto& op_create = op->Cast<duckdb::LogicalCreate>();
            auto& info = op_create.info->Cast<duckdb::CreateViewInfo>();
            this->channel.info(std::format("Skip for no user defined type: View/{}.{}", info.schema, info.view_name));
        }
        break;
    default:
        this->channel.warn(std::format("Unsupprted statement type: {}", magic_enum::enum_name(op->type)));
        break;
    }
}

auto resolveUserType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ZmqChannel& channel) -> std::optional<UserTypeEntry> {
    UserTypeEntry entry{};
    CreateOperatorVisitor visitor(entry, channel);
    visitor.VisitOperator(op);

    return std::move(visitor.handled ? std::make_optional(entry) : std::nullopt);
}

}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/matchers/catch_matchers_vector.hpp>

using namespace worker;
using namespace Catch::Matchers;

static auto runResolveUserTypeInternal(const std::string& sql, const std::vector<std::string>& schemas) -> std::optional<UserTypeEntry> {
    auto db = duckdb::DuckDB(nullptr);
    auto conn = duckdb::Connection(db);

    prepare_schema: {
        for (auto& schema: schemas) {
            conn.Query(schema);
        }
    }

    auto stmts = conn.ExtractStatements(sql);

    std::optional<UserTypeEntry> entry;
    try {
        conn.BeginTransaction();

        auto bound_statement = bindTypeToStatement(*conn.context, std::move(stmts[0]->Copy()));
        auto channel = ZmqChannel::unitTestChannel();
        entry = resolveUserType(bound_statement.plan, channel);

        conn.Commit();
    }
    catch (...) {
        conn.Rollback();
        throw;
    }

    return std::move(entry);
}

static auto runUnsuportedSchema(const std::string& sql, const std::vector<std::string>& schemas) -> void {
    auto entry = runResolveUserTypeInternal(sql, schemas);

    has_entry: {
        UNSCOPED_INFO("has no entry");
        REQUIRE_FALSE((bool)entry);
    }
}

static auto runResolveUserType(const std::string& sql, UserTypeKind kind, const std::string& type_name, const std::vector<UserTypeEntry::Member>& expect_fields) -> void {
    auto entry = runResolveUserTypeInternal(sql, {});

    has_entry: {
        UNSCOPED_INFO("has entry");
        REQUIRE((bool)entry);
    }
    type_kind: {
        UNSCOPED_INFO("User type kind");
        CHECK(entry.value().kind == kind);
    }
    type_name: {
        UNSCOPED_INFO("User type name");
        CHECK_THAT(entry.value().name, Equals(type_name));
    }
    fields: {
        UNSCOPED_INFO("field size");
        REQUIRE(entry.value().fields.size() == expect_fields.size());

        for (int i = 0; auto& expect: expect_fields) {
            auto& x = entry.value().fields[i];
            UNSCOPED_INFO("field#" << i+1);
            field_name: {
                UNSCOPED_INFO("field name");
                CHECK_THAT(x.field_name, Equals(expect.field_name));
            }
            field_type: {
                UNSCOPED_INFO("field type");
                CHECK(x.field_type.has_value() == expect.field_type.has_value());

                if (x.field_type.has_value()) {
                    CHECK_THAT(x.field_type.value(), Equals(expect.field_type.value()));
                }
            }
            ++i;
        }
    }
}

TEST_CASE("Unsupported schema#1 (table)") {
    std::string sql("create table Foo (id int primary key)");

    runUnsuportedSchema(sql, {});
}

TEST_CASE("Unsupported schema#2 (view)") {
    std::string sql("create view FooView (id) AS select * from Foo");
    std::string schema("create table Foo (id int primary key)");

    runUnsuportedSchema(sql, {schema});
}

TEST_CASE("Unsupported schema#3 (index)") {
    std::string sql("create index Idx_Foo on Foo (kind)");
    std::string schema("create table Foo (id int primary key, kind int not null)");

    runUnsuportedSchema(sql, {schema});
}

TEST_CASE("Unsupported schema#4 (sequence)") {
    std::string sql("create sequence Seq_Foo");

    runUnsuportedSchema(sql, {});
}

TEST_CASE("Extract enum type") {
    std::string sql("create type Visibility as ENUM ('hide', 'visible')");

    std::vector<UserTypeEntry::Member> expects{
        {.field_name = "hide", .field_type = std::nullopt},
        {.field_name = "visible", .field_type = std::nullopt},
    };
    runResolveUserType(sql, UserTypeKind::Enum, "Visibility", expects);
}

#endif
