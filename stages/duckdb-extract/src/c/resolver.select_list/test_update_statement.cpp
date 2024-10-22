#ifndef DISABLE_CATCH2_TEST

#include <catch2/catch_test_macros.hpp>

#include "run.hpp"

using namespace worker;

TEST_CASE("SelectList::Update statement") {
    SECTION("whereless") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            update Foo 
            set xys = $v1
        )#");
        std::vector<ColumnEntry> expects{};
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1}, expects, user_type_names, anon_types);
    }
    SECTION("basic/has returning (single column)") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            update Foo 
            set xys = $v1, remarks = $remarks
            where kind = $kind
            returning id
        )#");
        std::vector<ColumnEntry> expects{
            {.field_name = "id", .type_kind = UserTypeKind::Primitive, .field_type = "INTEGER", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1}, expects, user_type_names, anon_types);
    }
    SECTION("basic/has returning (single column/nullable)") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            update Foo 
            set xys = $v1, remarks = $remarks
            where kind = $kind
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
            update Foo 
            set xys = $v1, remarks = $remarks
            where kind = $kind
            returning kind, remarks
        )#");
        std::vector<ColumnEntry> expects{
            {.field_name = "kind", .type_kind = UserTypeKind::Primitive, .field_type = "INTEGER", .nullable = false},
            {.field_name = "remarks", .type_kind = UserTypeKind::Primitive, .field_type = "VARCHAR", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1}, expects, user_type_names, anon_types);
    }
    SECTION("has returning/tuple") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            update Foo 
            set xys = $v1, remarks = $remarks
            where kind = $kind
            returning (xys, id) as updated
        )#");
        std::vector<ColumnEntry> expects{
            {.field_name = "updated", .type_kind = UserTypeKind::Struct, .field_type = "SelList::Struct#1", .nullable = true},
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
