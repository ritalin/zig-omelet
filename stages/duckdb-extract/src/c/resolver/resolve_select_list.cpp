#include <ranges>

#include <duckdb.hpp>
#include <duckdb/planner/logical_operator_visitor.hpp>
#include <duckdb/planner/expression/list.hpp>
#include <duckdb/planner/tableref/list.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>

#include "duckdb_logical_visitors.hpp"
#include "user_type_support.hpp"

#include <iostream>
#include <magic_enum/magic_enum.hpp>

namespace worker {

auto ColumnNameVisitor::VisitReplace(duckdb::BoundConstantExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    this->column_name = expr.value.ToSQLString();
    return nullptr;
}

auto ColumnNameVisitor::Resolve(duckdb::unique_ptr<duckdb::Expression>& expr) -> std::string {
    if (expr->alias != "") {
        return expr->alias;
    }

    ColumnNameVisitor visitor;
    visitor.VisitExpression(&expr);
    
    return std::move(visitor.column_name);
}

auto resolveColumnTypeInternal(duckdb::unique_ptr<duckdb::LogicalOperator>& op, const ColumnNullableLookup& join_lookup, ZmqChannel& channel) -> ColumnResolveResult {
    std::vector<ColumnEntry> columns;
    columns.reserve(op->expressions.size());

    std::vector<std::string> user_type_names;
    std::vector<UserTypeEntry> anon_types;

    switch (op->type) {
    case duckdb::LogicalOperatorType::LOGICAL_ORDER_BY:
        {
            if (op->children.size() > 0) {
                return resolveColumnTypeInternal(op->children[0], join_lookup, channel);
            }
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_UNION:
    case duckdb::LogicalOperatorType::LOGICAL_INTERSECT:
    case duckdb::LogicalOperatorType::LOGICAL_EXCEPT:
        // Set-operation Can determin with left-side only
        return resolveColumnTypeInternal(op->children[0], join_lookup, channel);
    case duckdb::LogicalOperatorType::LOGICAL_MATERIALIZED_CTE: 
        return resolveColumnTypeInternal(op->children[1], join_lookup, channel);
    case duckdb::LogicalOperatorType::LOGICAL_PROJECTION: 
        {
            auto& op_projection = op->Cast<duckdb::LogicalProjection>();
            std::unordered_multiset<std::string> name_dupe{};

            for (size_t i = 0; auto& expr: op->expressions) {
                std::string type_name;
                anon_types: {
                    if (isEnumUserType(expr->return_type)) {
                        auto user_type_name = userTypeName(expr->return_type);
                        if (user_type_name != "") {
                            type_name = user_type_name;
                            user_type_names.push_back(user_type_name);
                        }
                        else {
                            type_name = std::format("SelList::Enum#{}", i+1);
                            anon_types.push_back(pickEnumUserType(expr->return_type, type_name));
                        }
                    }
                    else if (isArrayUserType(expr->return_type)) {
                        auto user_type_name = userTypeName(expr->return_type);
                        if (user_type_name != "") {
                            type_name = user_type_name;
                            user_type_names.push_back(user_type_name);
                        }
                        else {
                            type_name = std::format("SelList::Array#{}", i+1);
                            anon_types.push_back(pickArrayUserType(expr->return_type, type_name, user_type_names));
                        }
                    }
                    else if (isAliasUserType(expr->return_type)) {
                        type_name = userTypeName(expr->return_type);
                        if (type_name != "") {
                            user_type_names.push_back(type_name);
                        }
                    }
                    else {
                        type_name = expr->return_type.ToString();
                    }
                }
                columns: {
                    ColumnNullableLookup::Column binding{
                        .table_index = op_projection.table_index, 
                        .column_index = i,
                    };
                    auto field_name = ColumnNameVisitor::Resolve(expr);

                    auto dupe_count = name_dupe.count(field_name);
                    name_dupe.insert(field_name);

                    auto entry = ColumnEntry{
                        .field_name = dupe_count == 0 ? field_name : std::format("{}_{}", field_name, dupe_count),
                        .field_type = type_name,
                        .nullable = join_lookup[binding].shouldNulls(),
                    };

                    columns.emplace_back(std::move(entry));
                }
                ++i;
            }
        }
        break;
    default:
        channel.warn(std::format("[TODO] Can not resolve column type: {}", magic_enum::enum_name(op->type)));
        break;
    }

    return {
        .columns = std::move(columns),
        .user_type_names = std::move(user_type_names),
        .anon_types = std::move(anon_types),
    };
}

auto resolveColumnType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, StatementType stmt_type, duckdb::Connection& conn, ZmqChannel& channel) -> ColumnResolveResult {
    if (stmt_type != StatementType::Select) return {};

    auto join_types = resolveSelectListNullability(op, conn, channel);

    return resolveColumnTypeInternal(op, join_types, channel);
}

}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <utility>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/matchers/catch_matchers_vector.hpp>

#include "duckdb_database.hpp"

using namespace worker;
using namespace Catch::Matchers;

using LogicalOperatorRef = duckdb::unique_ptr<duckdb::LogicalOperator>;
using BoundTableRef = duckdb::unique_ptr<duckdb::BoundTableRef>;

static auto expectAnonymousUserType(UserTypeEntry actual, UserTypeEntry expect, size_t i, size_t depth) -> void {
    INFO(std::format("[{}] Anonymous type#{}", depth, i));
    user_type_kind: {
        UNSCOPED_INFO("user type kind");
        CHECK(actual.kind == expect.kind);
    }
    user_type_name: {
        UNSCOPED_INFO("user type name");
        CHECK_THAT(actual.name, Equals(expect.name));
    }
    user_type_field_size: {
        UNSCOPED_INFO("user type field size");
        REQUIRE(actual.fields.size() == expect.fields.size());
    }
    user_type_fields: {
        for (int j = 0; auto& field: actual.fields) {
            INFO(std::format("type field#{} of Anonymous type#{}", j, i));
            field_name: {
                UNSCOPED_INFO("user type field name");
                CHECK_THAT(field.field_name, Equals(expect.fields[j].field_name));
            }
            field_type: {
                UNSCOPED_INFO("has user type field type");
                REQUIRE((bool)field.field_type == (bool)expect.fields[j].field_type);

                if (field.field_type) {
                    expectAnonymousUserType(*field.field_type, *expect.fields[j].field_type, j, depth+1);
                }
            }
            ++j;
        }
    }
}

static auto runBindStatement(
    const std::string sql, const std::vector<std::string>& schemas, 
    const std::vector<ColumnEntry>& expects, 
    const std::vector<std::string>& user_type_names,
    const std::vector<UserTypeEntry>& anon_types) -> void 
{
    auto db = Database();
    auto conn = db.connect();

    prepare_schema: {
        for (auto& schema: schemas) {
            conn.Query(schema);
        }
        db.retainUserTypeName(conn);
    }

    ColumnResolveResult column_result;
    try {
        conn.BeginTransaction();

        auto stmts = conn.ExtractStatements(sql);
        auto stmt_type = evalStatementType(stmts[0]);

        auto bound_result = bindTypeToStatement(*conn.context, std::move(stmts[0]->Copy()), {}, {});

        auto channel = ZmqChannel::unitTestChannel();
        column_result = resolveColumnType(bound_result.stmt.plan, stmt_type, conn, channel);
        conn.Commit();
    }
    catch (...) {
        conn.Rollback();
        throw;
    }

    result_size: {
        INFO("Result size");
        REQUIRE(column_result.columns.size() == expects.size());
    }
    result_entries: {
        for (int i = 0; auto& entry: column_result.columns) {
            INFO(std::format("entry#{} (`{}`)", i+1, expects[i].field_name));
            field_name: {
                INFO("field name");
                CHECK_THAT(entry.field_name, Equals(expects[i].field_name));
            }
            field_type: {
                INFO("field type");
                CHECK_THAT(entry.field_type, Equals(expects[i].field_type));
            }
            nullable: {
                INFO("nullable");
                CHECK(entry.nullable == expects[i].nullable);
            }
            ++i;
        }
    }
    user_types_size: {
        INFO("user types size");
        REQUIRE(column_result.user_type_names.size() == user_type_names.size());
    }
    user_types: {
        for (auto& expect_name: user_type_names) {
            INFO(std::format("has user type name (`{}`)", expect_name));
            CHECK_THAT(column_result.user_type_names, VectorContains(expect_name));
        }
    }
    anonymous_types_size: {
        INFO("Anonymous types size");
        REQUIRE(column_result.anon_types.size() == anon_types.size());
    }
    anonymous_types: {
        for (int i = 0; auto& anon: column_result.anon_types) {
            expectAnonymousUserType(anon, anon_types[i], i, 0);
            ++i;
        }
    }
}

TEST_CASE("SelectList::Unsupported DML Statement") {
    SECTION("Insert Statement") {
        std::string sql("insert into Foo values (42, 1, null, 'misc...')");
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

        runBindStatement(sql, {schema}, {}, {}, {});
    }
    SECTION("Update Statement") {
        std::string sql("update Foo set kind = 2, xys = 101 where id = 42");
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

        runBindStatement(sql, {schema}, {}, {}, {});
    }
    SECTION("Delete Statement") {
        std::string sql("delete from Foo where id = 42");
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

        runBindStatement(sql, {schema}, {}, {}, {});
    }
}

TEST_CASE("SelectList::basic") {
    SECTION("basic") {
        std::string sql("select 123 as a, 98765432100 as b, 'abc' as c");
        std::vector<ColumnEntry> expects{
            {.field_name = "a", .field_type = "INTEGER", .nullable = false},
            {.field_name = "b", .field_type = "BIGINT", .nullable = false},
            {.field_name = "c", .field_type = "VARCHAR", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {}, expects, user_type_names, anon_types);
    }
    SECTION("without alias") {
        std::string sql("select 123, 98765432100, 'abc'");
        std::vector<ColumnEntry> expects{
            {.field_name = "123", .field_type = "INTEGER", .nullable = false},
            {.field_name = "98765432100", .field_type = "BIGINT", .nullable = false},
            {.field_name = "'abc'", .field_type = "VARCHAR", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {}, expects, user_type_names, anon_types);
    }
    SECTION("with null#1") {
        std::string sql("select 123 as a, 98765432100 as b, null::date as c");
        std::vector<ColumnEntry> expects{
            {.field_name = "a", .field_type = "INTEGER", .nullable = false},
            {.field_name = "b", .field_type = "BIGINT", .nullable = false},
            {.field_name = "c", .field_type = "DATE", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {}, expects, user_type_names, anon_types);
    }
    SECTION("with null#2") {
        std::string sql("select 123 + null as a, 98765432100 as b, 'abc' || null as c");
        std::vector<ColumnEntry> expects{
            {.field_name = "a", .field_type = "INTEGER", .nullable = true},
            {.field_name = "b", .field_type = "BIGINT", .nullable = false},
            {.field_name = "c", .field_type = R"#("NULL")#", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {}, expects, user_type_names, anon_types);
    }
    SECTION("with null#3") {
        std::string sql("select (null) is not false as a, null is null as b, null is not null as c");
        std::vector<ColumnEntry> expects{
            {.field_name = "a", .field_type = "BOOLEAN", .nullable = false},
            {.field_name = "b", .field_type = "BOOLEAN", .nullable = false},
            {.field_name = "c", .field_type = "BOOLEAN", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {}, expects, user_type_names, anon_types);
    }
    SECTION("has Parameters") {
        auto sql = std::string(R"#(SELECT CAST($1 AS INTEGER) AS a, CAST($2 AS VARCHAR) AS "CAST($v AS VARCHAR)")#");

        std::vector<ColumnEntry> expects{
            {.field_name = "a", .field_type = "INTEGER", .nullable = true},
            {.field_name = "CAST($v AS VARCHAR)", .field_type = "VARCHAR", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {}, expects, user_type_names, anon_types);
    }
}

TEST_CASE("SelectList::has expression") {
    SECTION("with coalesce#1") {
        std::string sql("select coalesce(null, null, 10) as a");
        std::vector<ColumnEntry> expects{
            {.field_name = "a", .field_type = "INTEGER", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {}, expects, user_type_names, anon_types);
    }
    SECTION("with coalesce#2") {
        std::string sql("select coalesce(null, null, null)::VARCHAR as a");
        std::vector<ColumnEntry> expects{
            {.field_name = "a", .field_type = "VARCHAR", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {}, expects, user_type_names, anon_types);
    }
    SECTION("with coalesce#3") {
        std::string sql("select coalesce(null, 42, null) as a");
        std::vector<ColumnEntry> expects{
            {.field_name = "a", .field_type = "INTEGER", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {}, expects, user_type_names, anon_types);
    }
    SECTION("with unary op") {
        std::string sql("select -42 as a");
        std::vector<ColumnEntry> expects{
            {.field_name = "a", .field_type = "INTEGER", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {}, expects, user_type_names, anon_types);
    }
    SECTION("with unary op with null") {
        std::string sql("select -(null)::int as a");
        std::vector<ColumnEntry> expects{
            {.field_name = "a", .field_type = "INTEGER", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {}, expects, user_type_names, anon_types);
    }
    SECTION("with scalar function call") {
        std::string sql("select concat('hello ', 'world ') as fn");
        std::vector<ColumnEntry> expects{
            {.field_name = "fn", .field_type = "VARCHAR", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {}, expects, user_type_names, anon_types);
    }
}

TEST_CASE("SelectList::case expression") {
    SECTION("case expression#1") {
        std::string sql(R"#(
            select (case when kind > 0 then kind else -kind end) as xyz from Foo
        )#");
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

        std::vector<ColumnEntry> expects{
            {.field_name = "xyz", .field_type = "INTEGER", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema}, expects, user_type_names, anon_types);
    }
    SECTION("case expression#2") {
        std::string sql(R"#(
            select (case kind when 0 then id + 10000 else id end) as xyz from Foo
        )#");
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

        std::vector<ColumnEntry> expects{
            {.field_name = "xyz", .field_type = "INTEGER", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema}, expects, user_type_names, anon_types);
    }
    SECTION("case expression#3") {
        std::string sql(R"#(
            select (case when kind > 0 then xys else xys end) as xyz from Foo
        )#");
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

        std::vector<ColumnEntry> expects{
            {.field_name = "xyz", .field_type = "INTEGER", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema}, expects, user_type_names, anon_types);
    }
    SECTION("case expression#4 (without else part)") {
        std::string sql(R"#(
            select (case when kind > 0 then kind end) as xyz from Foo
        )#");
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

        std::vector<ColumnEntry> expects{
            {.field_name = "xyz", .field_type = "INTEGER", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema}, expects, user_type_names, anon_types);
    }
}

TEST_CASE("SelectList::projection") {
    SECTION("with star expr") {
        std::string sql("select * from Foo");
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

        std::vector<ColumnEntry> expects{
            {.field_name = "id", .field_type = "INTEGER", .nullable = false},
            {.field_name = "kind", .field_type = "INTEGER", .nullable = false},
            {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
            {.field_name = "remarks", .field_type = "VARCHAR", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema}, expects, user_type_names, anon_types);
    }
    SECTION("projection") {
        std::string sql("select kind, xys from Foo");
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

        std::vector<ColumnEntry> expects{
            {.field_name = "kind", .field_type = "INTEGER", .nullable = false},
            {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema}, expects, user_type_names, anon_types);
    }
    SECTION("unordered column") {
        std::string sql("select kind, xys, id from Foo");
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

        std::vector<ColumnEntry> expects{
            {.field_name = "kind", .field_type = "INTEGER", .nullable = false},
            {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
            {.field_name = "id", .field_type = "INTEGER", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema}, expects, user_type_names, anon_types);
    }
    SECTION("with order by") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select kind, xys, id from Foo order by kind, id");

        std::vector<ColumnEntry> expects{
            {.field_name = "kind", .field_type = "INTEGER", .nullable = false},
            {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
            {.field_name = "id", .field_type = "INTEGER", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema}, expects, user_type_names, anon_types);
    }
}

TEST_CASE("SelectList::has table join") {
    SECTION("has inner join") {
        std::string sql(R"#(
            select Foo.id, Foo.remarks, Bar.value
            from Foo 
            join Bar on Foo.id = Bar.id and Bar.value <> $2
            where Foo.kind = $3
        )#");
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");

        std::vector<ColumnEntry> expects{
            {.field_name = "id", .field_type = "INTEGER", .nullable = false},
            {.field_name = "remarks", .field_type = "VARCHAR", .nullable = true},
            {.field_name = "value", .field_type = "VARCHAR", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1, schema_2}, expects, user_type_names, anon_types);
    }
    SECTION("unordered columns") {
        std::string sql(R"#(
            select Foo.id, Bar.value, Foo.remarks
            from Foo 
            join Bar on Foo.id = Bar.id and Bar.value <> $2
            where Foo.kind = $3
        )#");
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");

        std::vector<ColumnEntry> expects{
            {.field_name = "id", .field_type = "INTEGER", .nullable = false},
            {.field_name = "value", .field_type = "VARCHAR", .nullable = false},
            {.field_name = "remarks", .field_type = "VARCHAR", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1, schema_2}, expects, user_type_names, anon_types);
    }
    SECTION("with star expr") {
        std::string sql(R"#(
            select *
            from Foo 
            join Bar on Foo.id = Bar.id and Bar.value <> $2
            where Foo.kind = $3
        )#");
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");

        std::vector<ColumnEntry> expects{
            {.field_name = "id", .field_type = "INTEGER", .nullable = false},
            {.field_name = "kind", .field_type = "INTEGER", .nullable = false},
            {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
            {.field_name = "remarks", .field_type = "VARCHAR", .nullable = true},
            {.field_name = "id_1", .field_type = "INTEGER", .nullable = false},
            {.field_name = "value", .field_type = "VARCHAR", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1, schema_2}, expects, user_type_names, anon_types);
    }
    SECTION("with star expr partially") {
        std::string sql(R"#(
            select Foo.id, Bar.*
            from Foo 
            join Bar on Foo.id = Bar.id and Bar.value <> $2
            where Foo.kind = $3
        )#");
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");

        std::vector<ColumnEntry> expects{
            {.field_name = "id", .field_type = "INTEGER", .nullable = false},
            {.field_name = "id_1", .field_type = "INTEGER", .nullable = false},
            {.field_name = "value", .field_type = "VARCHAR", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1, schema_2}, expects, user_type_names, anon_types);
    }
    SECTION("has left join") {
        std::string sql(R"#(
            select *
            from Foo 
            left outer join Bar on Foo.id = Bar.id and Bar.value <> $2
            where Foo.kind = $3
        )#");
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");

        std::vector<ColumnEntry> expects{
            {.field_name = "id", .field_type = "INTEGER", .nullable = false},
            {.field_name = "kind", .field_type = "INTEGER", .nullable = false},
            {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
            {.field_name = "remarks", .field_type = "VARCHAR", .nullable = true},
            {.field_name = "id_1", .field_type = "INTEGER", .nullable = true},
            {.field_name = "value", .field_type = "VARCHAR", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1, schema_2}, expects, user_type_names, anon_types);
    }
    SECTION("has right join") {
        std::string sql(R"#(
            select *
            from Foo 
            right join Bar on Foo.id = Bar.id and Bar.value <> $2
            where Foo.kind = $3
        )#");
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");

        std::vector<ColumnEntry> expects{
            {.field_name = "id", .field_type = "INTEGER", .nullable = true},
            {.field_name = "kind", .field_type = "INTEGER", .nullable = true},
            {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
            {.field_name = "remarks", .field_type = "VARCHAR", .nullable = true},
            {.field_name = "id_1", .field_type = "INTEGER", .nullable = false},
            {.field_name = "value", .field_type = "VARCHAR", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1, schema_2}, expects, user_type_names, anon_types);
    }
    SECTION("has full outer join") {
        std::string sql(R"#(
            select *
            from Foo 
            full outer join Bar on Foo.id = Bar.id and Bar.value <> $2
            where Foo.kind = $3
        )#");
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");

        std::vector<ColumnEntry> expects{
            {.field_name = "id", .field_type = "INTEGER", .nullable = true},
            {.field_name = "kind", .field_type = "INTEGER", .nullable = true},
            {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
            {.field_name = "remarks", .field_type = "VARCHAR", .nullable = true},
            {.field_name = "id_1", .field_type = "INTEGER", .nullable = true},
            {.field_name = "value", .field_type = "VARCHAR", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1, schema_2}, expects, user_type_names, anon_types);
    }
    SECTION("has cross join") {
        std::string sql(R"#(
            select *
            from Foo 
            cross join Bar
            where Foo.kind = $3
        )#");
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");

        std::vector<ColumnEntry> expects{
            {.field_name = "id", .field_type = "INTEGER", .nullable = false},
            {.field_name = "kind", .field_type = "INTEGER", .nullable = false},
            {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
            {.field_name = "remarks", .field_type = "VARCHAR", .nullable = true},
            {.field_name = "id_1", .field_type = "INTEGER", .nullable = false},
            {.field_name = "value", .field_type = "VARCHAR", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1, schema_2}, expects, user_type_names, anon_types);
    }
    SECTION("has positional join") {
        std::string sql(R"#(
            select *
            from Foo 
            positional join Bar
            where Foo.kind = $3
        )#");
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");

        std::vector<ColumnEntry> expects{
            {.field_name = "id", .field_type = "INTEGER", .nullable = true},
            {.field_name = "kind", .field_type = "INTEGER", .nullable = true},
            {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
            {.field_name = "remarks", .field_type = "VARCHAR", .nullable = true},
            {.field_name = "id_1", .field_type = "INTEGER", .nullable = true},
            {.field_name = "value", .field_type = "VARCHAR", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1, schema_2}, expects, user_type_names, anon_types);
    }
}

TEST_CASE("SelectList::subquery") {
    SECTION("scalar subquery") {
        std::string sql(R"#(
            select 
                Foo.id,
                (
                    select Bar.value from Bar
                    where bar.id = Foo.id
                ) as v
            from Foo 
        )#");
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");

        std::vector<ColumnEntry> expects{
            {.field_name = "id", .field_type = "INTEGER", .nullable = false},
            {.field_name = "v", .field_type = "VARCHAR", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1, schema_2}, expects, user_type_names, anon_types);
    }
    SECTION("derived table#1") {
        std::string sql(R"#(
            select 
                v.*
            from (
                select id, xys, CAST($1 AS VARCHAR) From Foo
            ) v
        )#");
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

        std::vector<ColumnEntry> expects{
            {.field_name = "id", .field_type = "INTEGER", .nullable = false},
            {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
            {.field_name = "CAST($1 AS VARCHAR)", .field_type = "VARCHAR", .nullable = true},        
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1}, expects, user_type_names, anon_types);
    }
}

TEST_CASE("SelectList::table function") {
    SECTION("") {
        
    }
}

TEST_CASE("SelectList::user type (ENUM)") {
    SECTION("with placeholder of enum user type") {
        std::string schema("CREATE TYPE Visibility as ENUM ('hide','visible')");
        std::string sql("select $vis::Visibility as vis");

        std::vector<ColumnEntry> expects{
            {.field_name = "vis", .field_type = "Visibility", .nullable = true},
        };
        std::vector<std::string> user_type_names{"Visibility"};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema}, expects, user_type_names, anon_types);
    }
    SECTION("with placeholder of anonymous enum user type") {
        std::string schema_1("CREATE TYPE Visibility as ENUM ('hide','visible')");
        std::string sql("select $vis::ENUM('hide','visible') as vis");

        std::vector<ColumnEntry> expects{
            {.field_name = "vis", .field_type = "SelList::Enum#1", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{
            {.kind = UserTypeKind::Enum, .name = "SelList::Enum#1", .fields = {UserTypeEntry::Member("hide"), UserTypeEntry::Member("visible")}}
        }; 

        runBindStatement(sql, {schema_1}, expects, user_type_names, anon_types);
    }
    SECTION("with enum user type#1") {
        std::string schema_1("CREATE TYPE Visibility as ENUM ('hide','visible')");
        std::string schema_2("CREATE TABLE Control (id INTEGER primary key, name VARCHAR not null, vis Visibility not null)");
        std::string sql("select vis, id from Control");

        std::vector<ColumnEntry> expects{
            {.field_name = "vis", .field_type = "Visibility", .nullable = false},
            {.field_name = "id", .field_type = "INTEGER", .nullable = false},
        };
        std::vector<std::string> user_type_names{"Visibility"};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1, schema_2}, expects, user_type_names, anon_types);
    }
    SECTION("with enum user type#2") {
        std::string schema_1("CREATE TYPE Visibility as ENUM ('hide','visible')");
        std::string schema_2("CREATE TYPE Visibility2 as Visibility");
        std::string schema_3("CREATE TABLE Control (id INTEGER primary key, name VARCHAR not null, vis Visibility2 not null)");
        std::string sql("select vis, id from Control");

        std::vector<ColumnEntry> expects{
            {.field_name = "vis", .field_type = "Visibility2", .nullable = false},
            {.field_name = "id", .field_type = "INTEGER", .nullable = false},
        };
        std::vector<std::string> user_type_names{"Visibility2"};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1, schema_2, schema_3}, expects, user_type_names, anon_types);
    }
}

TEST_CASE("SelectList::user type (ALIAS)") {

    SECTION("basic") {
        std::string schema_1("CREATE TYPE Description AS VARCHAR");
        std::string schema_2("CREATE TABLE Summary (id INTEGER primary key, desc_text Description, remarks VARCHAR)");
        std::string sql("select desc_text::Description as desc_text, remarks from Summary");

        std::vector<ColumnEntry> expects{
            // {.field_name = "id", .field_type = "INTEGER", .nullable = false},
            {.field_name = "desc_text", .field_type = "Description", .nullable = true},
            {.field_name = "remarks", .field_type = "VARCHAR", .nullable = true},
        };
        std::vector<std::string> user_type_names{"Description"};
        std::vector<UserTypeEntry> anon_types{};

        runBindStatement(sql, {schema_1, schema_2}, expects, user_type_names, anon_types);
    }
}

TEST_CASE("SelectList::user type (LIST)") {
    SECTION("primitive list") {
        std::string schema("CREATE TABLE Toto (id INTEGER primary key, numbers INTEGER[] not null)");
        std::string sql("select id, numbers from Toto");

        std::vector<ColumnEntry> expects{
            {.field_name = "id", .field_type = "INTEGER", .nullable = false},
            {.field_name = "numbers", .field_type = "SelList::Array#2", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{
            {.kind = UserTypeKind::Array, .name = "SelList::Array#2", .fields = {
                UserTypeEntry::Member("Anon::Primitive#1", std::make_shared<UserTypeEntry>(UserTypeEntry{ .kind = UserTypeKind::Primitive, .name = "INTEGER", .fields = {}}))
            }},
        };

        runBindStatement(sql, {schema}, expects, user_type_names, anon_types);
    }
    SECTION("predefined enum list") {
        std::string schema_1("CREATE TYPE Visibility as ENUM ('hide','visible')");
        std::string schema_2("CREATE TABLE Element (id INTEGER primary key, child_visibles Visibility[] not null)");
        std::string sql("select child_visibles, id from Element");

        std::vector<ColumnEntry> expects{
            {.field_name = "child_visibles", .field_type = "SelList::Array#1", .nullable = false},
            {.field_name = "id", .field_type = "INTEGER", .nullable = false},
        };
        std::vector<std::string> user_type_names{"Visibility"};
        std::vector<UserTypeEntry> anon_types{
            {.kind = UserTypeKind::Array, .name = "SelList::Array#1", .fields = {
                UserTypeEntry::Member("Anon::Enum#1", std::make_shared<UserTypeEntry>(UserTypeEntry{ .kind = UserTypeKind::Enum, .name = "Visibility", .fields = {}}))
            }},
        };

        runBindStatement(sql, {schema_1, schema_2}, expects, user_type_names, anon_types);
    }
    SECTION("anonymous enum list") {
        SKIP("Anonymous enum list is bound as enum type !!!");
        std::string schema("CREATE TABLE Element (id INTEGER primary key, child_visibles ENUM('hide', 'visible')[] not null)");
        std::string sql("select child_visibles, id from Element");

        std::vector<ColumnEntry> expects{
            {.field_name = "child_visibles", .field_type = "SelList::Array#1", .nullable = false},
            {.field_name = "id", .field_type = "INTEGER", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{
            {.kind = UserTypeKind::Array, .name = "SelList::Array#1", .fields = {
                UserTypeEntry::Member("Anon::Enum#1", std::make_shared<UserTypeEntry>(UserTypeEntry{ .kind = UserTypeKind::Enum, .name = "Anon::Enum#1", .fields = {
                    UserTypeEntry::Member("hide"), UserTypeEntry::Member("visible")
                }}))
            }},
        };

        runBindStatement(sql, {schema}, expects, user_type_names, anon_types);
    }
}

TEST_CASE("SelectList::materialized CTE") {
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string sql(R"#(
        with v as materialized (
            select id, xys, kind from Foo
        )
        select xys, id from v
    )#");

    std::vector<ColumnEntry> expects{
        {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
        {.field_name = "id", .field_type = "INTEGER", .nullable = false},
    };
    std::vector<std::string> user_type_names{};
    std::vector<UserTypeEntry> anon_types{};
   
    runBindStatement(sql, {schema}, expects, user_type_names, anon_types);
}

TEST_CASE("SelectList::Recursive CTE") {
    SECTION("CTE") {
        std::string sql(R"#(
            with recursive t(n, k) AS (
                VALUES (1::bigint, $min_value::int)
                UNION ALL
                SELECT n+1, k-1 FROM t WHERE n < $max_value::int
            )
            SELECT k, n FROM t
        )#");

        std::vector<ColumnEntry> expects{
            {.field_name = "k", .field_type = "INTEGER", .nullable = true},
            {.field_name = "n", .field_type = "BIGINT", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};
    
        runBindStatement(sql, {}, expects, user_type_names, anon_types);
    }
    SECTION("default CTEx2") {
        std::string sql(R"#(
            with recursive 
                t(n, k) AS (
                    VALUES (0::bigint, current_date)
                    UNION ALL
                    SELECT n+1, k+2 FROM t WHERE n < $max_value::int
                ),
                t2(m, h) AS (
                    VALUES ($min_value::int, $val::int)
                    UNION ALL
                    SELECT m*2, h+1 FROM t2 WHERE m < $max_value2::int
                )
            SELECT n, h, k, m FROM t cross join t2
        )#");

        std::vector<ColumnEntry> expects{
            {.field_name = "n", .field_type = "BIGINT", .nullable = false},
            {.field_name = "h", .field_type = "INTEGER", .nullable = true},
            {.field_name = "k", .field_type = "DATE", .nullable = true},
            {.field_name = "m", .field_type = "INTEGER", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};
    
        runBindStatement(sql, {}, expects, user_type_names, anon_types);
    }
    SECTION("default CTE (nested)") {
        std::string sql(R"#(
            with recursive
                t(n, k) AS (
                    VALUES (0, 42::bigint)
                    UNION ALL
                    SELECT n+1, k*2 FROM t WHERE n < $max_value::int
                ),
                t2(m, h) as (
                    select n + $delta::int as m, k from t
                    union all
                    select m*2, h-1 from t2 where m < $max_value2::int
                )
            SELECT h, m FROM t2 
        )#");

        std::vector<ColumnEntry> expects{
            {.field_name = "h", .field_type = "BIGINT", .nullable = false},
            {.field_name = "m", .field_type = "INTEGER", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};
    
        runBindStatement(sql, {}, expects, user_type_names, anon_types);
    }
}

TEST_CASE("SelectList::Recursive materialized CTE") {
    SECTION("Matirialized CTE") {
        std::string sql(R"#(
            with recursive t(n, k) AS materialized (
                VALUES (1::bigint, $min_value::int)
                UNION ALL
                SELECT n+1, k-1 FROM t WHERE n < $max_value::int
            )
            SELECT k, n FROM t
        )#");

        std::vector<ColumnEntry> expects{
            {.field_name = "k", .field_type = "INTEGER", .nullable = true},
            {.field_name = "n", .field_type = "BIGINT", .nullable = false},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};
    
        runBindStatement(sql, {}, expects, user_type_names, anon_types);
    }
    SECTION("default CTEx2") {
        std::string sql(R"#(
            with recursive 
                t(n, k) AS materialized (
                    VALUES (0::bigint, current_date)
                    UNION ALL
                    SELECT n+1, k+2 FROM t WHERE n < $max_value::int
                ),
                t2(m, h) AS (
                    VALUES ($min_value::int, $val::int)
                    UNION ALL
                    SELECT m*2, h+1 FROM t2 WHERE m < $max_value2::int
                )
            SELECT n, h, k, m FROM t cross join t2
        )#");

        std::vector<ColumnEntry> expects{
            {.field_name = "n", .field_type = "BIGINT", .nullable = false},
            {.field_name = "h", .field_type = "INTEGER", .nullable = true},
            {.field_name = "k", .field_type = "DATE", .nullable = true},
            {.field_name = "m", .field_type = "INTEGER", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};
    
        runBindStatement(sql, {}, expects, user_type_names, anon_types);
    }
    SECTION("default CTE (nested)") {
        std::string sql(R"#(
            with recursive
                t(n, k) AS materialized (
                    VALUES (0, 42::bigint)
                    UNION ALL
                    SELECT n+1, k*2 FROM t WHERE n < $max_value::int
                ),
                t2(m, h) as (
                    select n + $delta::int as m, k from t
                    union all
                    select m*2, h-1 from t2 where m < $max_value2::int
                )
            SELECT h, m FROM t2 
        )#");

        std::vector<ColumnEntry> expects{
            {.field_name = "h", .field_type = "BIGINT", .nullable = false},
            {.field_name = "m", .field_type = "INTEGER", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};
    
        runBindStatement(sql, {}, expects, user_type_names, anon_types);
    }
}

TEST_CASE("SelectList::combining operation") {
    SECTION("union") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int, value VARCHAR not null)");
        std::string sql(R"#(
            select id from Foo where id > $n1
            union all
            select id from Bar where id <= $n2
        )#");

        std::vector<ColumnEntry> expects{
            {.field_name = "id", .field_type = "INTEGER", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};
    
        runBindStatement(sql, {schema_1, schema_2}, expects, user_type_names, anon_types);
    }
    SECTION("intersect") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int, value VARCHAR not null)");
        std::string sql(R"#(
            select id from Bar where id > $n1
            intersect all
            select id from Foo where id <= $n2
        )#");

        std::vector<ColumnEntry> expects{
            {.field_name = "id", .field_type = "INTEGER", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};
    
        runBindStatement(sql, {schema_1, schema_2}, expects, user_type_names, anon_types);
    }
    SECTION("except") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int, value VARCHAR not null)");
        std::string sql(R"#(
            select id from Bar where id > $n1
            except all
            select id from Foo where id <= $n2
        )#");

        std::vector<ColumnEntry> expects{
            {.field_name = "id", .field_type = "INTEGER", .nullable = true},
        };
        std::vector<std::string> user_type_names{};
        std::vector<UserTypeEntry> anon_types{};
    
        runBindStatement(sql, {schema_1, schema_2}, expects, user_type_names, anon_types);
    }
}

#endif