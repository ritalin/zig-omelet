#ifndef DISABLE_CATCH2_TEST

#include <catch2/catch_test_macros.hpp>

#include "run.hpp"

using namespace worker;

TEST_CASE("ResolveNullable::fromless") {
    SECTION("basic") {
        std::string sql(R"#(
            select 123, 'abc'
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {}, expects);
    }
}

TEST_CASE("ResolveNullable::joinless") {
    SECTION("basic") {
        std::string schema("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Bar
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema}, expects);
    }
    SECTION("unordered select list") {
        std::string schema("CREATE TABLE Bar (id int primary key, value VARCHAR not null, remarks VARCHAR)");
        std::string sql(R"#(
            select value, remarks, id from Bar
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(1, 2), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema}, expects);
    }
    SECTION("with unary op of nallble") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select -xys from Foo");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = true, .from_join = false}},
        };

        runResolveSelectListNullability(sql, {schema}, expects);
    }
    SECTION("aggregate with filter") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select sum($val::int) filter (fmod(id, $div::int) > $rem::int) as a from Foo");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = true, .from_join = false}},
        };

        runResolveSelectListNullability(sql, {schema}, expects);
    }
}

TEST_CASE("ResolveNullable::Inner join") {
    SECTION("basic") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            join Bar on Foo.id = Bar.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 4), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 5), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("nullable key") {
        std::string schema_1("CREATE TABLE Foo (id int, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            join Bar on Foo.id = Bar.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 5), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("join twice") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            join Bar b1 on Foo.id = b1.id
            join Bar b2 on Foo.id = b2.id    
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 4), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 5), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 6), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 7), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
}

TEST_CASE("ResolveNullable::outer join") {
    SECTION("left outer") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            left outer join Bar on Foo.id = Bar.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 5), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Left outer join twice") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            left outer join Bar b1 on Foo.id = b1.id
            left outer join Bar b2 on Foo.id = b2.id    
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 5), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 6), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 7), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Right outer") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            right outer join Bar on Foo.id = Bar.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 1), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 2), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 3), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 4), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 5), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
}

TEST_CASE("ResolveNullable::Inner + outer") {
    SECTION("Inner -> outer") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            join Bar b1 on Foo.id = b1.id
            left outer join Bar b2 on Foo.id = b2.id    
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 4), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 5), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 6), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 7), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Outer -> inner#1") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            left outer join Bar b1 on Foo.id = b1.id
            join Bar b2 on Foo.id = b2.id    
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 5), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 6), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 7), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Outer -> inner#2") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            left outer join Bar b1 on Foo.id = b1.id
            join Bar b2 on b1.id = b2.id    
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 5), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 6), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 7), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
}

TEST_CASE("ResolveNullable::scalar subquery") {
    SECTION("single left outer join") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select 
                Foo.id,
                (
                    select Bar.value from Bar
                    where bar.id = Foo.id
                ) as v
            from Foo 
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
}

TEST_CASE("ResolveNullable::Cross join") {
    SECTION("basic") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            cross join Bar
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 4), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 5), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Cross -> outer") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            cross join Bar b1
            left outer join Bar b2 on Foo.id = b2.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 4), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 5), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(3, 6), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 7), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
}

TEST_CASE("ResolveNullable::Full outer join") {
    SECTION("basic") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            full outer join Bar on Foo.id = Bar.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 1), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 2), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 3), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 5), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Inner -> full outer#1") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            join Bar b1 on Foo.id = b1.id
            full outer join Bar b2 on Foo.id = b2.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 2), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 3), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 5), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 6), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 7), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Inner -> full outer#2") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            join Bar b1 on Foo.id = b1.id
            full outer join Bar b2 on b1.id = b2.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 2), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 3), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 5), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 6), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 7), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Full outer -> inner#1") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            full outer join Bar b2 on Foo.id = b2.id
            join Bar b1 on Foo.id = b1.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 2), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 3), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 5), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 6), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 7), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Full outer -> inner#2") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            full outer join Bar b2 on Foo.id = b2.id
            join Bar b1 on b2.id = b1.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(3, 0), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 1), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 2), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 3), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 5), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 6), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(3, 7), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
}

TEST_CASE("ResolveNullable::Positional join") {
    SECTION("basic") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            positional join Bar
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 1), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 2), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 3), .nullable = {.from_field = true, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 5), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
}

TEST_CASE("ResolveNullable::lateral join") {
    SECTION("Inner join lateral#1") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select *
            from Foo 
            join lateral (
            select * from Bar
            where Bar.value = Foo.id
            ) on true
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(8, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 4), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 5), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Inner join lateral#2 (unordered select list)") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select v.k1, v.value, Foo.*, v.k2
            from Foo 
            join lateral (
            select value, id as k1, id as k2 from Bar
            where Bar.value = Foo.id
            ) v on true
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(8, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 2), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 3), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 4), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 5), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 6), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Inner join lateral#3 (nullable key)") {
        std::string schema_1("CREATE TABLE Foo (id int, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select *
            from Foo 
            join lateral (
            select * from Bar
            where Bar.value = Foo.id
            ) on true
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(8, 0), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(8, 5), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Outer join lateral") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select *
            from Foo 
            left outer join lateral (
            select * from Bar
            where Bar.value = Foo.id
            ) on true
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(8, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(8, 5), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
}

TEST_CASE("join + subquery") {
    SECTION("Inner join with subquery#1") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select *
            from Foo 
            join (
            select * from Bar
            ) b1 on b1.value = Foo.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(8, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 4), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 5), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("Inner join with subquery#2") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string schema_3("CREATE TABLE Baz (id int primary key, order_date DATE not null)");
        std::string sql(R"#(
            select *
            from Foo 
            join (
            select * from Bar
            join Baz on Bar.id = Baz.id
            ) b1 on b1.value = Foo.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(9, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(9, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(9, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(9, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(9, 4), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(9, 5), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(9, 6), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(9, 7), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2, schema_3}, expects);
    }
}

TEST_CASE("ResolveNullable::exists clause") {
    SECTION("mark join") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string schema_3("CREATE TABLE Baz (id int primary key, order_date DATE not null)");
        std::string sql(R"#(
            with ph as (select $k::int as k)
            select Bar.* from Bar
            join lateral (
                select * from Baz
                cross join ph
                where 
                    Baz.id = Bar.id
                    and exists (
                        from Foo 
                        where 
                            Foo.id = Baz.id
                            and kind = ph.k
                    )
            ) v on true
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(22, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(22, 1), .nullable = {.from_field = false, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2, schema_3}, expects);
    }
}

TEST_CASE("ResolveNullable::With order by query") {
    SECTION("basic") {
        std::string schema_1("CREATE TABLE Foo (id int, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from Foo
            join Bar on Foo.id = Bar.id
            order by Foo.id, bar.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 5), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("subquery with order by clause") {
        std::string schema_1("CREATE TABLE Foo (id int, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select * from (
                select * from Foo
                order by Foo.id
            ) v
            join Bar on v.id = Bar.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(8, 0), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 1), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(8, 4), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(8, 5), .nullable = {.from_field = false, .from_join = true} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
}

TEST_CASE("ResolveNullable::With group by query") {
    SECTION("basic") {
        std::string schema_1("CREATE TABLE Foo (id int, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
        std::string sql(R"#(
            select Bar.id, Foo.id, count(Foo.kind) as k, count(xys) as s from Foo
            join Bar on Foo.id = Bar.id
            group by Bar.id, Foo.id
        )#");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = false, .from_join = true} },
            { .binding = duckdb::ColumnBinding(2, 1), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(2, 3), .nullable = {.from_field = true, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("rollup") {
        std::string schema("CREATE TABLE Point (id int, x int not null, y int, z int not null)");
        std::string sql("SELECT x, y, GROUPING(x, y), GROUPING(x), sum(z) FROM Point GROUP BY ROLLUP(x, y)");

        std::vector<ColumnBindingPair> expects{
            { .binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false} },
            { .binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(1, 2), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(1, 3), .nullable = {.from_field = true, .from_join = false} },
            { .binding = duckdb::ColumnBinding(1, 4), .nullable = {.from_field = true, .from_join = false} },
        };

        runResolveSelectListNullability(sql, {schema}, expects);
    }
}

TEST_CASE("ResolveNullable::Builtin unnest function") {
    SECTION("Expand list#1") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select 42::int as x, xys, unnest([1, 2, 3, 5, 7, 11]) from Foo");

        // Note: list initializer([]) is a alias for list_value function, and is not operator.
        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 2), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
    SECTION("Expand list#2") {
        std::string sql("select 42::int as x, unnest('[1, 2, 3, 5, 7]'::json::int[]) as x");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("Result of generate_series") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select null::int as x, xys, unnest(generate_series(1, 20, 3)) from Foo");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 2), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
}

TEST_CASE("ResolveNullable::Builtin window function") {
    SECTION("row_number") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select id, row_number() over (order by id desc) as a from Foo");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
    SECTION("ntile") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select xys, ntile($bucket) over () as a from Foo");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
    SECTION("lag") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql("select id, lag(id, $offset, $value_def::int) over (partition by kind) as a from Foo");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
}

TEST_CASE("ResolveNullable::table function") {
    SECTION("read_json#1 struct") {
        std::string sql(R"#(
            select 
                unnest(h, recursive := true), 
                unnest(h), 
                t.* 
            from read_json("$dataset" := '_dataset-examples/struct_sample.json') t(h, i, j)
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 2), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 3), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 4), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 5), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 6), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 7), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("read_json#2 (derived table/unnest in outermost)") {
        std::string sql(R"#(
            select 
                unnest(v.h, recursive := true), unnest(v.h), v.* 
            from (
                select * 
                from read_json("$dataset" := '_dataset-examples/struct_sample.json') t(h, i, j)
            ) v
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(7, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 1), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 2), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 3), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 4), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 5), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 6), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 7), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("read_json#3 (derived table/unnest anything)") {
        std::string sql(R"#(
            select 
                unnest(data_1, recursive := true), unnest(data_2)
            from (
                select unnest(j)
                from read_json("$dataset" := '_dataset-examples/struct_sample_2.json') t(j)
            )
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(7, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 1), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 2), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 3), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 4), .nullable = {.from_field = true, .from_join = false}},
        };

        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("read_json#3 (derived table/unnest descendant directly)") {
        std::string sql(R"#(
            select 
                unnest(j.data_1.v)
            from read_json("$dataset" := '_dataset-examples/struct_sample_2.json') t(j)
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("read_json#5 list") {
        std::string sql(R"#(
            select 
                k, v1, unnest(v1), v2, v3, unnest(v3)
            from read_json("$dataset" := '_dataset-examples/list_sample.json') t(k, v1, v2, v3)
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 1), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 2), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 3), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 4), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(1, 5), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("read_json#6 from CTE#1") {
        std::string sql(R"#(
            with source as (
                select *
                from read_json("$dataset" := '_dataset-examples/struct_sample_2.json') t(j)
            )
            select unnest(s.j.data_1, recursive := true), unnest(s.j.data_2)
            from source s
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(7, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 1), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 2), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 3), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 4), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("read_json#6 from CTE#2") {
        std::string sql(R"#(
            with source as (
                select unnest(j)
                from read_json("$dataset" := '_dataset-examples/struct_sample_2.json') t(j)
            )
            select unnest(s.data_1, recursive := true), unnest(s.data_2)
            from source s
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(7, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 1), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 2), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 3), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 4), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("range#1") {
        std::string sql(R"#(
            select u.*
            from range("$start" := 1::bigint, "$stop" := 50, 3) u(id)
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(1, 0), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("range#2 (correlated column)") {
        std::string sql(R"#(
            select t.*, u.id
            from (values (1, 2), (2, 3)) t(x, y)
            cross join lateral range(x, "$stop" := 50, y) u(id)
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(15, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(15, 1), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(15, 2), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
}

TEST_CASE("ResolveNullable::CTE") {
    SECTION("default") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            with v as (
                select id, xys, kind from Foo
            )
            select xys, id from v
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(7, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 1), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
    SECTION("With non materialized CTE") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            with v as not materialized (
                select id, xys, kind from Foo
            )
            select xys, id from v
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(7, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(7, 1), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
}

TEST_CASE("ResolveNullable::Materialized CTE") {
    SECTION("basic") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            with v as materialized (
                select id, xys, kind from Foo
            )
            select xys, id from v
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(9, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(9, 1), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
    SECTION("CTEx2") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int, value VARCHAR not null)");
        std::string schema_3("CREATE TABLE Point (id int, x int not null, y int, z int not null)");
        std::string sql(R"#(
            with
                v as materialized (
                    select Foo.id, Bar.id, xys, kind, a from Foo
                    join Bar on Foo.id = Bar.id
                    cross join (
                        select $a::int as a
                    )
                ),
                v2 as materialized (
                    select $b::text as b, x from Point
                )
            select xys, id, b, x, a from v
            cross join v2
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(26, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(26, 1), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(26, 2), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(26, 3), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(26, 4), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema_1, schema_2, schema_3}, expects);
    }
    SECTION("nested CTE") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int, value VARCHAR not null)");
        std::string sql(R"#(
            with
                v as materialized (
                    select Foo.id, Bar.id, xys, kind, a from Foo
                    join Bar on Foo.id = Bar.id
                    cross join (
                        select $a::int as a
                    )
                ),
                v2 as materialized (
                    select id, id_1 from v
                )
            select id_1, id from v2
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(25, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(25, 1), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("reference from subquery") {
        std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            with v as materialized (
                select id, xys, kind from Foo
            )
            select xys, id from (
                select * from v
            )
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(15, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(15, 1), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema}, expects);
    }
}

TEST_CASE("ResolveNullable::Recursive CTE") {
    SECTION("default CTE") {
        std::string sql(R"#(
            with recursive t(n, k) AS (
                VALUES (0, $target_date::date)
                UNION ALL
                SELECT n+1, k+1 FROM t WHERE n < $max_value::int
            )
            SELECT k, n FROM t
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(15, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(15, 1), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("default CTEx2") {
        std::string sql(R"#(
            with recursive 
                t(n, k) AS (
                    VALUES (0, current_date)
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

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(30, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(30, 1), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(30, 2), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(30, 3), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("default CTE (nested)") {
        std::string sql(R"#(
            with recursive
                t(n, k) AS (
                    VALUES (0, 42)
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

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(29, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(29, 1), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
}

TEST_CASE("ResolveNullable::Recursive materialized CTE") {
    SECTION("Matirialized CTE") {
        std::string sql(R"#(
            with recursive t(n, k) AS materialized (
                VALUES (1, $min_value::int)
                UNION ALL
                SELECT n+1, k-1 FROM t WHERE n < $max_value::int
            )
            SELECT k, n FROM t
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(17, 0), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(17, 1), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("Matirialized CTEx2") {
        std::string sql(R"#(
            with recursive 
                t(n, k) AS materialized (
                    VALUES (0, current_date)
                    UNION ALL
                    SELECT n+1, k+2 FROM t WHERE n < $max_value::int
                ),
                t2(m, h) AS materialized (
                    VALUES ($min_value::int, $val::int)
                    UNION ALL
                    SELECT m*2, h+1 FROM t2 WHERE m < $max_value2::int
                )
            SELECT n, h, k, m FROM t cross join t2
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(34, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(34, 1), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(34, 2), .nullable = {.from_field = true, .from_join = false}},
            {.binding = duckdb::ColumnBinding(34, 3), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
    SECTION("materialized CTE (nested)") {
        std::string sql(R"#(
            with recursive
                t(n, k) AS materialized (
                    VALUES (0, 42)
                    UNION ALL
                    SELECT n+1, k*2 FROM t WHERE n < $max_value::int
                ),
                t2(m, h) as materialized (
                    select n + $delta::int as m, k from t
                    union all
                    select m*2, h-1 from t2 where m < $max_value2::int
                )
            SELECT h, m FROM t2 
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(33, 0), .nullable = {.from_field = false, .from_join = false}},
            {.binding = duckdb::ColumnBinding(33, 1), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {}, expects);
    }
}

TEST_CASE("ResolveNullable::With combining operation") {
    SECTION("union#1 (bottom nullable)") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int, value VARCHAR not null)");
        std::string sql(R"#(
            select id from Foo where id > $n1
            union all
            select id from Bar where id <= $n2
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("union#2") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string sql(R"#(
            select id from Foo where id > $n1
            union all
            select id from Foo where id <= $n2
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema_1}, expects);
    }
    SECTION("intersect#1 (bottom nullable)") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int, value VARCHAR not null)");
        std::string sql(R"#(
            select id from Foo where id > $n1
            intersect all
            select id from Bar where id <= $n2
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("intersect#2 (top nullable)") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int, value VARCHAR not null)");
        std::string sql(R"#(
            select id from Bar where id > $n1
            intersect all
            select id from Foo where id <= $n2
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("except#1 (top nullable)") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int, value VARCHAR not null)");
        std::string sql(R"#(
            select id from Foo where id > $n1
            except all
            select id from Bar where id <= $n2
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = false, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
    SECTION("except#2 (bottom nullable)") {
        std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
        std::string schema_2("CREATE TABLE Bar (id int, value VARCHAR not null)");
        std::string sql(R"#(
            select id from Bar where id > $n1
            except all
            select id from Foo where id <= $n2
        )#");

        std::vector<ColumnBindingPair> expects{
            {.binding = duckdb::ColumnBinding(2, 0), .nullable = {.from_field = true, .from_join = false}},
        };
    
        runResolveSelectListNullability(sql, {schema_1, schema_2}, expects);
    }
}

#endif
