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
    CreateOperatorVisitor(UserTypeEntry& entry_ref, std::vector<std::string>& user_types_ref, std::vector<UserTypeEntry>& anon_types_ref, ZmqChannel& channel_ref): 
        channel(channel_ref), entry(entry_ref), nested_user_types(user_types_ref), nested_anon_types(anon_types_ref)
    {
    }
public:
    auto VisitOperator(duckdb::unique_ptr<duckdb::LogicalOperator>& op) -> void;
private:
    ZmqChannel& channel;
    UserTypeEntry& entry;
    std::vector<std::string>& nested_user_types;
    std::vector<UserTypeEntry>& nested_anon_types;
};

auto CreateOperatorVisitor::VisitOperator(duckdb::unique_ptr<duckdb::LogicalOperator>& op) -> void {
    switch (op->type) {
    case duckdb::LogicalOperatorType::LOGICAL_CREATE_TYPE: 
        {
            auto& op_create = op->Cast<duckdb::LogicalCreate>();
            auto& info = op_create.info->Cast<duckdb::CreateTypeInfo>();
            auto index = AnonymousCounter(1).begin();

            switch (info.type.id()) {
            case duckdb::LogicalTypeId::ENUM: 
                {
                    this->handled = true;
                    this->entry = pickEnumUserType(info.type, info.name);
                }
                break;
            case duckdb::LogicalTypeId::STRUCT: 
                {
                    this->handled = true;
                    this->entry = pickStructUserType(info.type, info.name, this->nested_user_types, this->nested_anon_types, index);
                }
                break;
            default: 
                {
                    // TODO: not implemented
                    if (userTypeName(info.type) != "") {
                        this->channel.info(std::format("[TODO] Alias for user type is not implemented: Type/{}.{}", info.schema, info.name));
                    }
                    std::vector<std::string> user_type_names{};

                    this->handled = true;
                    this->entry = pickAliasUserType(info.type, info.name, user_type_names, index);
                }
                break;
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

auto resolveUserType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, ZmqChannel& channel) -> std::optional<UserTypeResult> {
    UserTypeEntry entry{};
    std::vector<std::string> nested_user_types{};
    std::vector<UserTypeEntry> anon_types{};

    CreateOperatorVisitor visitor(entry, nested_user_types, anon_types, channel);
    visitor.VisitOperator(op);

    if (visitor.handled) {
        return std::make_optional(UserTypeResult{ 
            .entry = std::move(entry), 
            .user_type_names = std::move(nested_user_types), 
            .anon_types = std::move(anon_types),
        });
    }
    else {
        return std::nullopt;
    }
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

using UserTypeExpects = std::vector<std::string>;
using AnonTypeExpects = std::vector<UserTypeEntry>;

static auto runResolveUserTypeInternal(const std::string& sql, const std::vector<std::string>& depend_schemas) -> std::optional<UserTypeResult> {
    auto db = Database();
    auto conn = db.connect();

    prepare_schema: {
        for (auto& schema: depend_schemas) {
            conn.Query(schema);
        }
        db.retainUserTypeName(conn);
    }

    auto stmts = conn.ExtractStatements(sql);

    std::optional<UserTypeResult> result;
    try {
        conn.BeginTransaction();

        auto bound_result = bindTypeToStatement(*conn.context, std::move(stmts[0]->Copy()), {}, {});
        auto channel = ZmqChannel::unitTestChannel();
        result = resolveUserType(bound_result.stmt.plan, channel);

        conn.Commit();
    }
    catch (...) {
        conn.Rollback();
        throw;
    }

    return std::move(result);
}

static auto runUnsuportedSchema(const std::string& sql, const std::vector<std::string>& schemas) -> void {
    auto entry = runResolveUserTypeInternal(sql, schemas);

    has_entry: {
        INFO("has no entry");
        REQUIRE_FALSE((bool)entry);
    }
}

static auto expectUserType(
    const UserTypeEntry& actial, 
    const UserTypeEntry& expect, 
    size_t depth) -> void 
{
    type_kind: {
        INFO(std::format("[{}] User type kind", depth));
        CHECK(magic_enum::enum_name(actial.kind) == magic_enum::enum_name(expect.kind));
    }
    type_name: {
        INFO(std::format("[{}] User type name", depth));
        CHECK_THAT(actial.name, Equals(expect.name));
    }
    fields: {
        INFO(std::format("[{}] field size", depth));
        REQUIRE(actial.fields.size() == expect.fields.size());

        for (int i = 0; auto& expect_field: expect.fields) {
            auto& x = actial.fields[i];
            INFO(std::format("[{}] field#{}", depth, i+1));
            field_name: {
                INFO(std::format("[{}] field name", depth));
                CHECK_THAT(x.field_name, Equals(expect_field.field_name));
            }
            field_type: {
                INFO(std::format("[{}] field type", depth));
                REQUIRE((bool)x.field_type == (bool)expect_field.field_type);

                if (x.field_type) {
                    expectUserType(*x.field_type, *expect_field.field_type, depth+1);
                }
            }
            ++i;
        }
    }
}

static auto runResolveUserType(
    const std::string& sql, 
    const std::vector<std::string> depend_schemas, 
    UserTypeKind kind, 
    const std::string& type_name, 
    const std::vector<UserTypeEntry::Member>& expect_fields,
    const UserTypeExpects& expect_user_type_names,
    const AnonTypeExpects& expect_anon_types) -> void 
{
    auto result = runResolveUserTypeInternal(sql, depend_schemas);

    has_entry: {
        INFO("has entry");
        REQUIRE((bool)result);
    }

    expectUserType(result.value().entry, {.kind = kind, .name = type_name, .fields = expect_fields}, 0);

    nested_user_type: {
        INFO("nested user type size");
        REQUIRE(result.value().user_type_names.size() == expect_user_type_names.size());

        for (int i = 0; auto& name: expect_user_type_names) {
            INFO(std::format("nested user type#{}", ++i));
            CHECK_THAT(result.value().user_type_names, VectorContains(name));
        }
    }
    anonymous_type: {
        INFO("anonymous user type size");
        REQUIRE(result.value().anon_types.size() == expect_anon_types.size());
        
        auto anon_type_view = result.value().anon_types | std::views::transform([](const auto& x) {
            return std::pair<std::string, UserTypeEntry>(x.name, x);
        });
        std::unordered_map<std::string, UserTypeEntry> anon_type_lookup(anon_type_view.begin(), anon_type_view.end());

        for (int i = 0; auto& expect: expect_anon_types) {
            {
                INFO(std::format("anonymous user type#{}", i+1));
                expectUserType(anon_type_lookup.at(expect.name), expect, 0);
            }
            ++i;
        }
    }
}

TEST_CASE("UserType::Unsupported schema") {
    SECTION("create table") {
        std::string sql("create table Foo (id int primary key)");

        runUnsuportedSchema(sql, {});
    }
    SECTION("create view") {
        std::string sql("create view FooView (id) AS select * from Foo");
        std::string schema("create table Foo (id int primary key)");

        runUnsuportedSchema(sql, {schema});
    }
    SECTION("create index") {
        std::string sql("create index Idx_Foo on Foo (kind)");
        std::string schema("create table Foo (id int primary key, kind int not null)");

        runUnsuportedSchema(sql, {schema});
    }
    SECTION("create sequence") {
        std::string sql("create sequence Seq_Foo");

        runUnsuportedSchema(sql, {});
    }
}

TEST_CASE("UserType::Extract enum type") {
    std::string sql("create type Visibility as ENUM ('hide', 'visible')");

    std::vector<UserTypeEntry::Member> expects{
        UserTypeEntry::Member("hide"),
        UserTypeEntry::Member("visible"),
    };
    UserTypeExpects nested_user_type_names{};
    AnonTypeExpects anon_types{};

    runResolveUserType(sql, {}, UserTypeKind::Enum, "Visibility", expects, nested_user_type_names, anon_types);
}

TEST_CASE("UserType::Extract struct type (primitive type only)") {
    SECTION("primitive member") {
        std::string sql("create type KV as Struct (key text, value int)");

        std::vector<UserTypeEntry::Member> expects{
            UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
            UserTypeEntry::Member("value", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "INTEGER", .fields = {}})),
        };
        UserTypeExpects nested_user_type_names{};
        AnonTypeExpects anon_types{};
    
        runResolveUserType(sql, {}, UserTypeKind::Struct, "KV", expects, nested_user_type_names, anon_types);
    }
    SECTION("primitive list member") {
        std::string sql("create type KV as Struct (key text, value int[])");

        std::vector<UserTypeEntry::Member> expects{
            UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
            UserTypeEntry::Member("value", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Array, .name = "Anon::Array#1", .fields = {}})),
        };
        UserTypeExpects nested_user_type_names{};
        AnonTypeExpects anon_types{
            UserTypeEntry{.kind = UserTypeKind::Array, .name = "Anon::Array#1", .fields = {
                UserTypeEntry::Member("Anon::Primitive#2", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "INTEGER", .fields = {}})),
            }}
        };
    
        runResolveUserType(sql, {}, UserTypeKind::Struct, "KV", expects, nested_user_type_names, anon_types);
    }
}

TEST_CASE("UserType::Extract struct type (has enum type)") {
    SECTION("predefined type") {
        std::string schema("create type Visibility as ENUM ('hide', 'visible')");
        std::string sql("create type KV as Struct (key text, vis Visibility)");

        std::vector<UserTypeEntry::Member> expects{
            UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
            UserTypeEntry::Member("vis", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::User, .name = "Visibility", .fields = {}})),
        };
        UserTypeExpects nested_user_type_names{"Visibility"};
        AnonTypeExpects anon_types{};
    
        runResolveUserType(sql, {schema}, UserTypeKind::Struct, "KV", expects, nested_user_type_names, anon_types);
    }
    SECTION("anonymous type#1") {
        std::string sql("create type KV as Struct (key text, vis ENUM ('hide', 'visible'))");

        std::vector<UserTypeEntry::Member> expects{
            UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
            UserTypeEntry::Member("vis", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Enum, .name = "Anon::Enum#1", .fields = {}})),
        };
        UserTypeExpects nested_user_type_names{};
        AnonTypeExpects anon_types{
            UserTypeEntry{.kind = UserTypeKind::Enum, .name = "Anon::Enum#1", .fields = {
                UserTypeEntry::Member("hide"),
                UserTypeEntry::Member("visible"),
            }}
        };
    
        runResolveUserType(sql, {}, UserTypeKind::Struct, "KV", expects, nested_user_type_names, anon_types);
    }
    SECTION("anonymous type#2") {
        std::string sql("create type KV as Struct (key text, vis ENUM ('hide', 'visible'), status ENUM ('failed', 'success'))");

        std::vector<UserTypeEntry::Member> expects{
            UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
            UserTypeEntry::Member("vis", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Enum, .name = "Anon::Enum#1", .fields = {}})),
            UserTypeEntry::Member("status", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Enum, .name = "Anon::Enum#2", .fields = {}})),
        };
        UserTypeExpects nested_user_type_names{};
        AnonTypeExpects anon_types{
            UserTypeEntry{.kind = UserTypeKind::Enum, .name = "Anon::Enum#1", .fields = {
                UserTypeEntry::Member("hide"),
                UserTypeEntry::Member("visible"),
            }},
            UserTypeEntry{.kind = UserTypeKind::Enum, .name = "Anon::Enum#2", .fields = {
                UserTypeEntry::Member("failed"),
                UserTypeEntry::Member("success"),
            }},
        };
    
        runResolveUserType(sql, {}, UserTypeKind::Struct, "KV", expects, nested_user_type_names, anon_types);
    }
}

TEST_CASE("UserType::Extract struct type (has struct type)") {
    SECTION("predefined type") {
        std::string schema("create type Child as Struct (key text, value int)");
        std::string sql("create type KV as Struct (key text, data Child)");

        std::vector<UserTypeEntry::Member> expects{
            UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
            UserTypeEntry::Member("data", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::User, .name = "Child", .fields = {}})),
        };
        UserTypeExpects nested_user_type_names{"Child"};
        AnonTypeExpects anon_types{};
        
        runResolveUserType(sql, {schema}, UserTypeKind::Struct, "KV", expects, nested_user_type_names, anon_types);
    }
    SECTION("single#1") {
        std::string sql("create type KV as Struct (key text, data STRUCT (id bigint, name text, age int))");

        std::vector<UserTypeEntry::Member> expects{
            UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
            UserTypeEntry::Member("data", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#1", .fields = {}})),
        };
        UserTypeExpects nested_user_type_names{};
        AnonTypeExpects anon_types{
            UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#1", .fields = {
                UserTypeEntry::Member("id", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "BIGINT", .fields = {}})),
                UserTypeEntry::Member("name", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
                UserTypeEntry::Member("age", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "INTEGER", .fields = {}})),
            }},
        };
        
        runResolveUserType(sql, {}, UserTypeKind::Struct, "KV", expects, nested_user_type_names, anon_types);
    }
    SECTION("single#2 (nested predefined user type)") {
        std::string schema("create type Gender as ENUM ('male', 'female', 'unknown')");
        std::string sql("create type KV as Struct (key text, data STRUCT (id bigint, name text, gender Gender))");

        std::vector<UserTypeEntry::Member> expects{
            UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
            UserTypeEntry::Member("data", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#1", .fields = {}})),
        };
        UserTypeExpects nested_user_type_names{"Gender"};
        AnonTypeExpects anon_types{
            UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#1", .fields = {
                UserTypeEntry::Member("id", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "BIGINT", .fields = {}})),
                UserTypeEntry::Member("name", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
                UserTypeEntry::Member("gender", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::User, .name = "Gender", .fields = {}})),
            }},
        };
        
        runResolveUserType(sql, {schema}, UserTypeKind::Struct, "KV", expects, nested_user_type_names, anon_types);
    }
    SECTION("nested#1 (nested anonymous enum type)") {
        std::string sql("create type KV as Struct (key text, data STRUCT (id bigint, name text, gender ENUM ('male', 'female', 'unknown')))");
        std::vector<UserTypeEntry::Member> expects{
            UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
            UserTypeEntry::Member("data", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#1", .fields = {}})),
        };
        UserTypeExpects nested_user_type_names{};
        AnonTypeExpects anon_types{
            UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#1", .fields = {
                UserTypeEntry::Member("id", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "BIGINT", .fields = {}})),
                UserTypeEntry::Member("name", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
                UserTypeEntry::Member("gender", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Enum, .name = "Anon::Enum#2", .fields = {}})),
            }},
            UserTypeEntry{.kind = UserTypeKind::Enum, .name = "Anon::Enum#2", .fields = {
                UserTypeEntry::Member("male"),
                UserTypeEntry::Member("female"),
                UserTypeEntry::Member("unknown"),
            }},
        };
        
        runResolveUserType(sql, {}, UserTypeKind::Struct, "KV", expects, nested_user_type_names, anon_types);
    }
    SECTION("nested#3 (nested predefined enum type list)") {
        std::string schema("create type Gender as ENUM ('male', 'female', 'unknown')");
        std::string sql("create type KV as Struct (key text, data STRUCT (id bigint, name text, gender Gender[]))");

        std::vector<UserTypeEntry::Member> expects{
            UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
            UserTypeEntry::Member("data", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#1", .fields = {}})),
        };
        UserTypeExpects nested_user_type_names{"Gender"};
        AnonTypeExpects anon_types{
            UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#1", .fields = {
                UserTypeEntry::Member("id", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "BIGINT", .fields = {}})),
                UserTypeEntry::Member("name", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
                UserTypeEntry::Member("gender", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Array, .name = "Anon::Array#2", .fields = {}})),
            }},
            UserTypeEntry{.kind = UserTypeKind::Array, .name = "Anon::Array#2", .fields = {
                UserTypeEntry::Member("Anon::User#3", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::User, .name = "Gender", .fields = {}})),
            }},
        };
        
        runResolveUserType(sql, {schema}, UserTypeKind::Struct, "KV", expects, nested_user_type_names, anon_types);
    }
    SECTION("nested#3 (nested predefined struct type list)") {
        std::string schema("create type Child as Struct (key text, value int)");
        std::string sql("create type KV as Struct (key text, data STRUCT (id bigint, name text, children Child[]))");

        std::vector<UserTypeEntry::Member> expects{
            UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
            UserTypeEntry::Member("data", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#1", .fields = {}})),
        };
        UserTypeExpects nested_user_type_names{"Child"};
        AnonTypeExpects anon_types{
            UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#1", .fields = {
                UserTypeEntry::Member("id", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "BIGINT", .fields = {}})),
                UserTypeEntry::Member("name", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
                UserTypeEntry::Member("children", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Array, .name = "Anon::Array#2", .fields = {}})),
            }},
            UserTypeEntry{.kind = UserTypeKind::Array, .name = "Anon::Array#2", .fields = {
                UserTypeEntry::Member("Anon::User#3", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::User, .name = "Child", .fields = {}})),
            }},
        };

        runResolveUserType(sql, {schema}, UserTypeKind::Struct, "KV", expects, nested_user_type_names, anon_types);
    }
    SECTION("nested#4 (nested anonymous struct type)") {
        std::string sql("create type KV as Struct (key text, data STRUCT (id bigint, name text, child STRUCT (key text, value int)))");

        std::vector<UserTypeEntry::Member> expects{
            UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
            UserTypeEntry::Member("data", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#1", .fields = {}})),
        };
        UserTypeExpects nested_user_type_names{};
        AnonTypeExpects anon_types{
            UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#1", .fields = {
                UserTypeEntry::Member("id", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "BIGINT", .fields = {}})),
                UserTypeEntry::Member("name", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
                UserTypeEntry::Member("child", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#2", .fields = {}})),
            }},
            UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#2", .fields = {
                UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
                UserTypeEntry::Member("value", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "INTEGER", .fields = {}})),
            }},
        };

        runResolveUserType(sql, {}, UserTypeKind::Struct, "KV", expects, nested_user_type_names, anon_types);
    }
    SECTION("nested#4 (nested anonymous struct type [x2])") {
        std::string sql(R"#(
            create type KV as Struct (
                key text, 
                data_1 STRUCT (id bigint, name text, child STRUCT (key text, value int)),
                data_2 STRUCT (id bigint, name text, child STRUCT (key text, value int))
            )
        )#");

        std::vector<UserTypeEntry::Member> expects{
            UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
            UserTypeEntry::Member("data_1", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#1", .fields = {}})),
            UserTypeEntry::Member("data_2", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#3", .fields = {}})),
        };
        UserTypeExpects nested_user_type_names{};
        AnonTypeExpects anon_types{
            UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#1", .fields = {
                UserTypeEntry::Member("id", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "BIGINT", .fields = {}})),
                UserTypeEntry::Member("name", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
                UserTypeEntry::Member("child", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#2", .fields = {}})),
            }},
            UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#2", .fields = {
                UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
                UserTypeEntry::Member("value", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "INTEGER", .fields = {}})),
            }},
            UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#3", .fields = {
                UserTypeEntry::Member("id", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "BIGINT", .fields = {}})),
                UserTypeEntry::Member("name", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
                UserTypeEntry::Member("child", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#4", .fields = {}})),
            }},
            UserTypeEntry{.kind = UserTypeKind::Struct, .name = "Anon::Struct#4", .fields = {
                UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
                UserTypeEntry::Member("value", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "INTEGER", .fields = {}})),
            }},
        };

        runResolveUserType(sql, {}, UserTypeKind::Struct, "KV", expects, nested_user_type_names, anon_types);
    }
}

TEST_CASE("UserType::Extract struct type (has list type)") {
    SECTION("primitive type item") {
        std::string sql("create type KV as Struct (key text, names text[])");

        std::vector<UserTypeEntry::Member> expects{
            UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
            UserTypeEntry::Member("names", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Array, .name = "Anon::Array#1", .fields = {}})),
        };
        UserTypeExpects nested_user_type_names{};
        AnonTypeExpects anon_types{
            UserTypeEntry{.kind = UserTypeKind::Array, .name = "Anon::Array#1", .fields = {
                UserTypeEntry::Member("Anon::Primitive#2", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
            }}
        };

        runResolveUserType(sql, {}, UserTypeKind::Struct, "KV", expects, nested_user_type_names, anon_types);
    }
    SECTION("predefined enum item#1") {
        std::string schema("create type Visibility as ENUM ('hide', 'visible')");
        std::string sql("create type KV as Struct (key text, vis Visibility[])");

        std::vector<UserTypeEntry::Member> expects{
            UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
            UserTypeEntry::Member("vis", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Array, .name = "Anon::Array#1", .fields = {}})),
        };
        UserTypeExpects nested_user_type_names{"Visibility"};
        AnonTypeExpects anon_types{
            UserTypeEntry{.kind = UserTypeKind::Array, .name = "Anon::Array#1", .fields = {
                UserTypeEntry::Member("Anon::User#2", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::User, .name = "Visibility", .fields = {}})),
            }}
        };

        runResolveUserType(sql, {schema}, UserTypeKind::Struct, "KV", expects, nested_user_type_names, anon_types);
    }
    SECTION("predefined enum item#2") {
        std::string schema("create type Visibility as ENUM ('hide', 'visible')");
        std::string sql("create type KV as Struct (key text, vis_1 Visibility[], vis_2 Visibility[])");

        std::vector<UserTypeEntry::Member> expects{
            UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
            UserTypeEntry::Member("vis_1", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Array, .name = "Anon::Array#1", .fields = {}})),
            UserTypeEntry::Member("vis_2", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Array, .name = "Anon::Array#3", .fields = {}})),
        };
        UserTypeExpects nested_user_type_names{"Visibility", "Visibility"};
        AnonTypeExpects anon_types{
            UserTypeEntry{.kind = UserTypeKind::Array, .name = "Anon::Array#1", .fields = {
                UserTypeEntry::Member("Anon::User#2", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::User, .name = "Visibility", .fields = {}})),
            }},
            UserTypeEntry{.kind = UserTypeKind::Array, .name = "Anon::Array#3", .fields = {
                UserTypeEntry::Member("Anon::User#4", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::User, .name = "Visibility", .fields = {}})),
            }},
        };

        runResolveUserType(sql, {schema}, UserTypeKind::Struct, "KV", expects, nested_user_type_names, anon_types);
    }
    SECTION("predefined struct member#1") {
        std::string schema("create type Child as Struct (key text, value int)");
        std::string sql("create type KV as Struct (key text, values Child[])");

        std::vector<UserTypeEntry::Member> expects{
            UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
            UserTypeEntry::Member("values", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Array, .name = "Anon::Array#1", .fields = {}})),
        };
        UserTypeExpects nested_user_type_names{"Child"};
        AnonTypeExpects anon_types{
            UserTypeEntry{.kind = UserTypeKind::Array, .name = "Anon::Array#1", .fields = {
                UserTypeEntry::Member("Anon::User#2", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::User, .name = "Child", .fields = {}})),
            }},
        };

        runResolveUserType(sql, {schema}, UserTypeKind::Struct, "KV", expects, nested_user_type_names, anon_types);
    }
    SECTION("predefined struct member#2") {
        std::string schema("create type Child as Struct (key text, value int)");
        std::string sql("create type KV as Struct (key text, values_1 Child[], values_2 Child[])");

        std::vector<UserTypeEntry::Member> expects{
            UserTypeEntry::Member("key", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}})),
            UserTypeEntry::Member("values_1", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Array, .name = "Anon::Array#1", .fields = {}})),
            UserTypeEntry::Member("values_2", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::Array, .name = "Anon::Array#3", .fields = {}})),
        };
        UserTypeExpects nested_user_type_names{"Child", "Child"};
        AnonTypeExpects anon_types{
            UserTypeEntry{.kind = UserTypeKind::Array, .name = "Anon::Array#1", .fields = {
                UserTypeEntry::Member("Anon::User#2", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::User, .name = "Child", .fields = {}})),
            }},
            UserTypeEntry{.kind = UserTypeKind::Array, .name = "Anon::Array#3", .fields = {
                UserTypeEntry::Member("Anon::User#4", std::make_shared<UserTypeEntry>(UserTypeEntry{.kind = UserTypeKind::User, .name = "Child", .fields = {}})),
            }},
        };

        runResolveUserType(sql, {schema}, UserTypeKind::Struct, "KV", expects, nested_user_type_names, anon_types);
    }
}

TEST_CASE("UserType::Extract alias type") {
    std::string sql("CREATE TYPE Description AS VARCHAR");

    std::vector<UserTypeEntry::Member> expects{
        UserTypeEntry::Member("Anon::Primitive#1", std::make_shared<UserTypeEntry>(UserTypeEntry{ .kind = UserTypeKind::Primitive, .name = "VARCHAR", .fields = {}}))
    };
    UserTypeExpects nested_user_type_names{};
    AnonTypeExpects anon_types{};
    
    runResolveUserType(sql, {}, UserTypeKind::Alias, "Description", expects, nested_user_type_names, anon_types);
}

#endif
