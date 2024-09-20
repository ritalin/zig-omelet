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

auto CreateOperatorVisitor::VisitOperator(duckdb::unique_ptr<duckdb::LogicalOperator>& op) -> void {
    switch (op->type) {
    case duckdb::LogicalOperatorType::LOGICAL_CREATE_TYPE: 
        {
            auto& op_create = op->Cast<duckdb::LogicalCreate>();
            auto& info = op_create.info->Cast<duckdb::CreateTypeInfo>();
            if (info.type.id() == duckdb::LogicalTypeId::ENUM) {
                this->handled = true;
                this->entry = pickEnumUserType(info.type, info.name);
            }
            else {
                // TODO not implemented
                if (userTypeName(info.type) != "") {
                    this->channel.info(std::format("[TODO] Alias for user type is not implemented: Type/{}.{}", info.schema, info.name));
                }
                std::vector<std::string> user_type_names{};

                this->handled = true;
                this->entry = pickAliasUserType(info.type, info.name, user_type_names);
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

    return visitor.handled ? std::make_optional<UserTypeEntry>(entry) : std::nullopt;
}

}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/matchers/catch_matchers_vector.hpp>

#include "duckdb_database.hpp"

using namespace worker;
using namespace Catch::Matchers;

static auto runResolveUserTypeInternal(const std::string& sql, const std::vector<std::string>& schemas) -> std::optional<UserTypeEntry> {
    auto db = Database();
    auto conn = db.connect();

    prepare_schema: {
        for (auto& schema: schemas) {
            conn.Query(schema);
        }
        db.retainUserTypeName(conn);
    }

    auto stmts = conn.ExtractStatements(sql);

    std::optional<UserTypeEntry> entry;
    try {
        conn.BeginTransaction();

        auto bound_result = bindTypeToStatement(*conn.context, std::move(stmts[0]->Copy()), {}, {});
        auto channel = ZmqChannel::unitTestChannel();
        entry = resolveUserType(bound_result.stmt.plan, channel);

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

static auto expectUserType(const UserTypeEntry& actial, const UserTypeEntry& expect, size_t depth) -> void {
    type_kind: {
        UNSCOPED_INFO(std::format("[{}] User type kind", depth));
        CHECK(actial.kind == expect.kind);
    }
    type_name: {
        UNSCOPED_INFO(std::format("[{}] User type name", depth));
        CHECK_THAT(actial.name, Equals(expect.name));
    }
    fields: {
        UNSCOPED_INFO(std::format("[{}] field size", depth));
        REQUIRE(actial.fields.size() == expect.fields.size());

        for (int i = 0; auto& expect_field: expect.fields) {
            auto& x = actial.fields[i];
            UNSCOPED_INFO(std::format("[{}] field#{}", depth, i+1));
            field_name: {
                UNSCOPED_INFO(std::format("[{}] field name", depth));
                CHECK_THAT(x.field_name, Equals(expect_field.field_name));
            }
            field_type: {
                UNSCOPED_INFO(std::format("[{}] field type", depth));
                CHECK((bool)x.field_type == (bool)expect_field.field_type);

                if (x.field_type) {
                    expectUserType(*x.field_type, *expect_field.field_type, depth+1);
                }
            }
            ++i;
        }
    }
}

static auto runResolveUserType(const std::string& sql, UserTypeKind kind, const std::string& type_name, const std::vector<UserTypeEntry::Member>& expect_fields) -> void {
    auto entry = runResolveUserTypeInternal(sql, {});

    has_entry: {
        UNSCOPED_INFO("has entry");
        REQUIRE((bool)entry);
    }
    expectUserType(entry.value(), {.kind = kind, .name = type_name, .fields = expect_fields}, 0);
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
        UserTypeEntry::Member("hide"),
        UserTypeEntry::Member("visible"),
    };
    runResolveUserType(sql, UserTypeKind::Enum, "Visibility", expects);
}

TEST_CASE("Extract alias type") {
    std::string sql("CREATE TYPE Description AS VARCHAR");

    std::vector<UserTypeEntry::Member> expects{
        UserTypeEntry::Member("Anon::Primitive#1", std::make_shared<UserTypeEntry>(UserTypeEntry{ .kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}}))
    };
    runResolveUserType(sql, UserTypeKind::Alias, "Description", expects);
}

#endif
