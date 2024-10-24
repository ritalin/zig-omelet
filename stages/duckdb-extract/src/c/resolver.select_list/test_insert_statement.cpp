#ifndef DISABLE_CATCH2_TEST

#include <catch2/catch_test_macros.hpp>

#include "run.hpp"

using namespace worker;

TEST_CASE("SelectList::Insert statement") {
    SECTION("basic") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            insert into Foo (id, kind) 
            values ($id, $kind)
        )#");
        std::vector<ColumnEntry> expects{};
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1}, expects, user_type_names, anon_types);
    }
    SECTION("has returning/single column") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            insert into Foo (id, kind) 
            values ($id, $kind)
            returning id
        )#");
        std::vector<ColumnEntry> expects{
            {.field_name = "id", .type_kind = UserTypeKind::Primitive, .field_type = "INTEGER", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1}, expects, user_type_names, anon_types);
    }
    SECTION("has returning/single column (nullable)") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            insert into Foo (id, kind) 
            values ($id, $kind)
            returning xys
        )#");
        std::vector<ColumnEntry> expects{
            {.field_name = "xys", .type_kind = UserTypeKind::Primitive, .field_type = "INTEGER", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1}, expects, user_type_names, anon_types);
    }
    SECTION("has returning/multi column") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            insert into Foo (id, kind) 
            values ($id, $kind)
            returning xys, id
        )#");
        std::vector<ColumnEntry> expects{
            {.field_name = "xys", .type_kind = UserTypeKind::Primitive, .field_type = "INTEGER", .nullable = true},
            {.field_name = "id", .type_kind = UserTypeKind::Primitive, .field_type = "INTEGER", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1}, expects, user_type_names, anon_types);
    }
    SECTION("has returning/anonymous tuple") {
        SKIP("Unsupported returning alias, currently");
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            insert into Foo (id, kind) 
            values ($id, $kind)
            returning (xys, id) as deleted
        )#");
        std::vector<ColumnEntry> expects{
            {.field_name = "deleted", .type_kind = UserTypeKind::Struct, .field_type = "SelList::Struct#1", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{
            {.kind = UserTypeKind::Struct, .name = "SelList::Struct#1", .fields = {
                UserTypeEntry::Member("0", std::make_shared<UserTypeEntry>(UserTypeEntry{ .kind = UserTypeKind::Primitive, .name = "INTEGER", .fields = {}})),
                UserTypeEntry::Member("1", std::make_shared<UserTypeEntry>(UserTypeEntry{ .kind = UserTypeKind::Primitive, .name = "INTEGER", .fields = {}})),
            }},
        };

        runBindStatement(sql, {schema_1}, expects, user_type_names, anon_types);
    }
    SECTION("has returning/anonymous tuple (without alias)") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            insert into Foo (id, kind) 
            values ($id, $kind)
            returning (xys, id)
        )#");
        std::vector<ColumnEntry> expects{
            {.field_name = R"#(main."row"(xys, id))#", .type_kind = UserTypeKind::Struct, .field_type = "SelList::Struct#1", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{
            {.kind = UserTypeKind::Struct, .name = "SelList::Struct#1", .fields = {
                UserTypeEntry::Member("0", std::make_shared<UserTypeEntry>(UserTypeEntry{ .kind = UserTypeKind::Primitive, .name = "INTEGER", .fields = {}})),
                UserTypeEntry::Member("1", std::make_shared<UserTypeEntry>(UserTypeEntry{ .kind = UserTypeKind::Primitive, .name = "INTEGER", .fields = {}})),
            }},
        };

        runBindStatement(sql, {schema_1}, expects, user_type_names, anon_types);
    }
    SECTION("has returning/anonymous tuple (with alias)") {
        SKIP("Unsupported returning alias, currently");
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            insert into Foo (id, kind) 
            values ($id, $kind)
            returning (xys, id) as inserted
        )#");
        std::vector<ColumnEntry> expects{
            {.field_name = R"#(main."row"(xys, id))#", .type_kind = UserTypeKind::Struct, .field_type = "SelList::Struct#1", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{
            {.kind = UserTypeKind::Struct, .name = "SelList::Struct#1", .fields = {
                UserTypeEntry::Member("0", std::make_shared<UserTypeEntry>(UserTypeEntry{ .kind = UserTypeKind::Primitive, .name = "INTEGER", .fields = {}})),
                UserTypeEntry::Member("1", std::make_shared<UserTypeEntry>(UserTypeEntry{ .kind = UserTypeKind::Primitive, .name = "INTEGER", .fields = {}})),
            }},
        };

        runBindStatement(sql, {schema_1}, expects, user_type_names, anon_types);
    }
}

#endif
