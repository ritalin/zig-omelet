#include <ranges>

#include <duckdb.hpp>
#include <duckdb/planner/logical_operator_visitor.hpp>
#include <duckdb/planner/expression/list.hpp>
#include <duckdb/planner/tableref/list.hpp>
// #include <duckdb/parser/constraints/list.hpp>
// #include <duckdb/catalog/catalog_entry/list.hpp>
// #include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>

#include "duckdb_logical_visitors.hpp"

#include <iostream>
#include <magic_enum/magic_enum.hpp>

namespace worker {

namespace column_name {
    class Visitor: public duckdb::LogicalOperatorVisitor {
    public:
        static auto Resolve(duckdb::unique_ptr<duckdb::Expression>& expr) -> std::string;
    protected:
        auto VisitReplace(duckdb::BoundConstantExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
            this->column_name = expr.value.ToSQLString();
            return nullptr;
        }
    private:
        std::string column_name;
    };

    auto Visitor::Resolve(duckdb::unique_ptr<duckdb::Expression>& expr) -> std::string {
        if (expr->alias != "") {
            return expr->alias;
        }

        Visitor visitor;
        visitor.VisitExpression(&expr);
        
        return std::move(visitor.column_name);
    }
}

auto resolveColumnTypeInternal(duckdb::unique_ptr<duckdb::LogicalOperator>& op, const NullableLookup& join_lookup) -> std::vector<ColumnEntry> {
    std::vector<ColumnEntry> result;
    result.reserve(op->expressions.size());

    if (op->type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
        auto& op_projection = op->Cast<duckdb::LogicalProjection>();
        std::unordered_multiset<std::string> name_dupe{};

        for (size_t i = 0; auto& expr: op->expressions) {
            NullableLookup::Column binding{
                .table_index = op_projection.table_index, 
                .column_index = i,
            };
            auto field_name = column_name::Visitor::Resolve(expr);

            auto dupe_count = name_dupe.count(field_name);
            name_dupe.insert(field_name);

            auto entry = ColumnEntry{
                .field_name = dupe_count == 0 ? field_name : std::format("{}_{}", field_name, dupe_count),
                .field_type = expr->return_type.ToString(),
                .nullable = join_lookup[binding].shouldNulls(),
            };
            // std::cout << std::format("Entry/name: {}, type: {}, nullable: {}", entry.field_name, entry.field_type, entry.nullable) << std::endl << std::endl;
            result.emplace_back(std::move(entry));
            ++i;
        }
    }

    return std::move(result);
}

auto resolveColumnType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, StatementType stmt_type) -> std::vector<ColumnEntry> {
    if (stmt_type != StatementType::Select) return {};

    auto join_types = createJoinTypeLookup(op);

    return resolveColumnTypeInternal(op, join_types);
}

}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <utility>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

using namespace worker;
using namespace Catch::Matchers;

using LogicalOperatorRef = duckdb::unique_ptr<duckdb::LogicalOperator>;
using BoundTableRef = duckdb::unique_ptr<duckdb::BoundTableRef>;

static auto runBindStatement(const std::string sql, const std::vector<std::string>& schemas, const std::vector<ColumnEntry>& expects) -> void {
    auto db = duckdb::DuckDB(nullptr);
    auto conn = duckdb::Connection(db);

    prepare_schema: {
        for (auto& schema: schemas) {
            conn.Query(schema);
        }
    }

    std::vector<ColumnEntry> column_result;
    try {
        conn.BeginTransaction();

        auto stmts = conn.ExtractStatements(sql);
        auto stmt_type = evalStatementType(stmts[0]);

        auto bound_statement = bindTypeToStatement(*conn.context, std::move(stmts[0]->Copy()));
        // auto bound_tables = bindTypeToTableRef(*conn.context, std::move(stmts[0]->Copy()), stmt_type);

        column_result = resolveColumnType(bound_statement.plan, stmt_type);
        conn.Commit();
    }
    catch (...) {
        conn.Rollback();
        throw;
    }

    result_size: {
        UNSCOPED_INFO("Result size");
        REQUIRE(column_result.size() == expects.size());
    }
    result_entries: {
        for (int i = 0; auto& entry: column_result) {
            INFO(std::format("entry#{} (`{}`)", i+1, expects[i].field_name));
            field_name: {
                UNSCOPED_INFO("field name");
                CHECK_THAT(entry.field_name, Equals(expects[i].field_name));
            }
            field_type: {
                UNSCOPED_INFO("field type");
                CHECK_THAT(entry.field_type, Equals(expects[i].field_type));
            }
            nullable: {
                UNSCOPED_INFO("nullable");
                CHECK(entry.nullable == expects[i].nullable);
            }
            ++i;
        }
    }
}

TEST_CASE("Insert Statement") {
    std::string sql("insert into Foo values (42, 1, null, 'misc...')");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    runBindStatement(sql, {schema}, {});
}

TEST_CASE("Update Statement") {
    std::string sql("update Foo set kind = 2, xys = 101 where id = 42");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    runBindStatement(sql, {schema}, {});
}

TEST_CASE("Delete Statement") {
    std::string sql("delete from Foo where id = 42");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    runBindStatement(sql, {schema}, {});
}

TEST_CASE("Select list only#1") {
    std::string sql("select 123 as a, 98765432100 as b, 'abc' as c");
    std::vector<ColumnEntry> expects{
        {.field_name = "a", .field_type = "INTEGER", .nullable = false},
        {.field_name = "b", .field_type = "BIGINT", .nullable = false},
        {.field_name = "c", .field_type = "VARCHAR", .nullable = false},
    };

    runBindStatement(sql, {}, expects);
}

TEST_CASE("Select constant list only#2 (without alias)") {
    std::string sql("select 123, 98765432100, 'abc'");
    std::vector<ColumnEntry> expects{
        {.field_name = "123", .field_type = "INTEGER", .nullable = false},
        {.field_name = "98765432100", .field_type = "BIGINT", .nullable = false},
        {.field_name = "'abc'", .field_type = "VARCHAR", .nullable = false},
    };

    runBindStatement(sql, {}, expects);
}

TEST_CASE("Select list only with null#1") {
    std::string sql("select 123 as a, 98765432100 as b, null::date as c");
    std::vector<ColumnEntry> expects{
        {.field_name = "a", .field_type = "INTEGER", .nullable = false},
        {.field_name = "b", .field_type = "BIGINT", .nullable = false},
        {.field_name = "c", .field_type = "DATE", .nullable = true},
    };

    runBindStatement(sql, {}, expects);
}

TEST_CASE("Select list only with null#2") {
    std::string sql("select 123 + null as a, 98765432100 as b, 'abc' || null as c");
    std::vector<ColumnEntry> expects{
        {.field_name = "a", .field_type = "INTEGER", .nullable = true},
        {.field_name = "b", .field_type = "BIGINT", .nullable = false},
        {.field_name = "c", .field_type = "VARCHAR", .nullable = true},
    };

    runBindStatement(sql, {}, expects);
}

TEST_CASE("Select list only with null#3") {
    std::string sql("select (null) is not false as a, null is null as b, null is not null as c");
    std::vector<ColumnEntry> expects{
        {.field_name = "a", .field_type = "BOOLEAN", .nullable = false},
        {.field_name = "b", .field_type = "BOOLEAN", .nullable = false},
        {.field_name = "c", .field_type = "BOOLEAN", .nullable = false},
    };

    runBindStatement(sql, {}, expects);
}

TEST_CASE("Select list only with coalesce#1") {
    std::string sql("select coalesce(null, null, 10) as a");
    std::vector<ColumnEntry> expects{
        {.field_name = "a", .field_type = "INTEGER", .nullable = false},
    };

    runBindStatement(sql, {}, expects);
}

TEST_CASE("Select list only with coalesce#2") {
    std::string sql("select coalesce(null, null, null)::VARCHAR as a");
    std::vector<ColumnEntry> expects{
        {.field_name = "a", .field_type = "VARCHAR", .nullable = true},
    };

    runBindStatement(sql, {}, expects);
}

TEST_CASE("Select list only with coalesce#3") {
    std::string sql("select coalesce(null, 42, null) as a");
    std::vector<ColumnEntry> expects{
        {.field_name = "a", .field_type = "INTEGER", .nullable = false},
    };

    runBindStatement(sql, {}, expects);
}

TEST_CASE("Select list only with unary op") {
    std::string sql("select -42 as a");
    std::vector<ColumnEntry> expects{
        {.field_name = "a", .field_type = "INTEGER", .nullable = false},
    };

    runBindStatement(sql, {}, expects);
}

TEST_CASE("Select list only with unary op with null") {
    std::string sql("select -(null)::int as a");
    std::vector<ColumnEntry> expects{
        {.field_name = "a", .field_type = "INTEGER", .nullable = true},
    };

    runBindStatement(sql, {}, expects);
}

TEST_CASE("Select list only with scalar function call") {
    std::string sql("select concat('hello ', 'world ') as fn");
    std::vector<ColumnEntry> expects{
        {.field_name = "fn", .field_type = "VARCHAR", .nullable = true},
    };

    runBindStatement(sql, {}, expects);
}

TEST_CASE("Parameter select list#1") {
    auto sql = std::string(R"#(SELECT CAST($1 AS INTEGER) AS a, CAST($2 AS VARCHAR) AS "CAST($v AS VARCHAR)")#");

    std::vector<ColumnEntry> expects{
        {.field_name = "a", .field_type = "INTEGER", .nullable = true},
        {.field_name = "CAST($v AS VARCHAR)", .field_type = "VARCHAR", .nullable = true},
    };

    runBindStatement(sql, {}, expects);
}

TEST_CASE("Select case expr#1") {
    std::string sql(R"#(
        select (case when kind > 0 then kind else -kind end) as xyz from Foo
    )#");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    std::vector<ColumnEntry> expects{
        {.field_name = "xyz", .field_type = "INTEGER", .nullable = false},
    };

    runBindStatement(sql, {schema}, expects);
}

TEST_CASE("Select case expr#2") {
    std::string sql(R"#(
        select (case kind when 0 then id + 10000 else id end) as xyz from Foo
    )#");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    std::vector<ColumnEntry> expects{
        {.field_name = "xyz", .field_type = "INTEGER", .nullable = false},
    };

    runBindStatement(sql, {schema}, expects);
}

TEST_CASE("Select case expr#3") {
    std::string sql(R"#(
        select (case when kind > 0 then xys else xys end) as xyz from Foo
    )#");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    std::vector<ColumnEntry> expects{
        {.field_name = "xyz", .field_type = "INTEGER", .nullable = true},
    };

    runBindStatement(sql, {schema}, expects);
}

TEST_CASE("Select case expr#3 (without else)") {
    std::string sql(R"#(
        select (case when kind > 0 then kind end) as xyz from Foo
    )#");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    std::vector<ColumnEntry> expects{
        {.field_name = "xyz", .field_type = "INTEGER", .nullable = true},
    };

    runBindStatement(sql, {schema}, expects);
}

TEST_CASE("Select from table#1 (with star expr)") {
    std::string sql("select * from Foo");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    std::vector<ColumnEntry> expects{
        {.field_name = "id", .field_type = "INTEGER", .nullable = false},
        {.field_name = "kind", .field_type = "INTEGER", .nullable = false},
        {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
        {.field_name = "remarks", .field_type = "VARCHAR", .nullable = true},
    };

    runBindStatement(sql, {schema}, expects);
}

TEST_CASE("Select from table#2 (projection)") {
    std::string sql("select kind, xys from Foo");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    std::vector<ColumnEntry> expects{
        {.field_name = "kind", .field_type = "INTEGER", .nullable = false},
        {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
    };

    runBindStatement(sql, {schema}, expects);
}

TEST_CASE("Select from table#3 (unordered column)") {
    std::string sql("select kind, xys, id from Foo");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    std::vector<ColumnEntry> expects{
        {.field_name = "kind", .field_type = "INTEGER", .nullable = false},
        {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
        {.field_name = "id", .field_type = "INTEGER", .nullable = false},
    };

    runBindStatement(sql, {schema}, expects);
}

TEST_CASE("Select from joined table#2") {
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

    runBindStatement(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Select from joined table#3 (unordered columns)") {
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

    runBindStatement(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Select from joined table#4 (with star expr)") {
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

    runBindStatement(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Select from joined table#5 (with star expr partially)") {
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

    runBindStatement(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Select from joined table#6 (with left join)") {
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

    runBindStatement(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Select from joined table#7 (with right join)") {
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

    runBindStatement(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Select from joined table#8 (with full outer join)") {
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

    runBindStatement(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Select from joined table#9 (with cross join)") {
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

    runBindStatement(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Select from joined table#10 (with positional join)") {
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

    runBindStatement(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Select from scalar subquery") {
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

    runBindStatement(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Select from derived table#1") {
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

    // runBindStatement(sql, {schema_1}, expects);
}

#ifdef ENABLE_TEST

TEST_CASE("Select from derived table join") {
    SKIP("Not implemented");
}

TEST_CASE("Select from table function") {
    SKIP("Not implemented");
}

#endif

#endif