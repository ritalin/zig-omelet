#include <algorithm>

#include <duckdb.hpp>
#include <duckdb/planner/logical_operator_visitor.hpp>
#include <duckdb/planner/expression/list.hpp>

#include "duckdb_params_collector.hpp"
#include "duckdb_binder_support.hpp"

namespace worker {

class LogicalParameterVisitor: public duckdb::LogicalOperatorVisitor {
public:
    LogicalParameterVisitor(const ParamNameLookup& lookup): lookup(lookup) {}
public:
    auto VisitResult() -> std::vector<ParamEntry>;
protected:
	auto VisitReplace(duckdb::BoundParameterExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
private:
    auto hasAnyType(const PositionalParam& position, const std::string& type_name) -> bool;
private:
    ParamNameLookup lookup;
    std::unordered_map<PositionalParam, ParamEntry> parameters;
    std::unordered_multimap<PositionalParam, std::string> param_types;
};

auto LogicalParameterVisitor::VisitResult() -> std::vector<ParamEntry> {
    auto view = this->parameters | std::views::values;
    
    std::vector<ParamEntry> result(view.begin(), view.end());
    std::ranges::sort(result, {}, &ParamEntry::sort_order);

    return result;
}

static inline auto hasAnyTypeInternal(const std::unordered_multimap<PositionalParam, std::string>& param_types, const PositionalParam& position, const std::string& type_name) -> bool {
    auto [iter_from, itr_to] = param_types.equal_range(position);
    auto view = std::ranges::subrange(iter_from, itr_to);

    auto pred_fn = [&type_name](std::pair<std::string, std::string> p) { 
        auto [k, v] = p;
        return v == type_name; 
    };
    if (std::ranges::any_of(view, pred_fn)) {
        return false;
    }

    return true;
}

auto LogicalParameterVisitor::hasAnyType(const PositionalParam& position, const std::string& type_name) -> bool {
    return hasAnyTypeInternal(this->param_types, position, type_name);
}

auto LogicalParameterVisitor::VisitReplace(duckdb::BoundParameterExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    auto type_name = expr.return_type.ToString();
    
    if (! this->parameters.contains(expr.identifier)) {
        this->parameters[expr.identifier] = ParamEntry{
            .position = expr.identifier,
            .name = this->lookup.at(expr.identifier),
            .type_name = type_name,
            .sort_order = std::stoul(expr.identifier)
        };
        this->param_types.insert(std::make_pair(expr.identifier, type_name));

        return nullptr;
    }

    if (this->hasAnyType(expr.identifier, type_name)) {
        this->parameters.at(expr.identifier).type_name = std::nullopt;
        this->param_types.insert(std::make_pair(expr.identifier, type_name));
    }

    return nullptr;
}

auto resolveParamType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, const ParamNameLookup& lookup) -> std::vector<ParamEntry> {
    LogicalParameterVisitor visitor(lookup);
    visitor.VisitOperator(*op);

    return visitor.VisitResult();
}

}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

using namespace worker;
using namespace Catch::Matchers;

using ParamTypeLookup = std::unordered_map<NamedParam, std::string>;

TEST_CASE("Typename checking#1 (same type)") {
    std::unordered_multimap<std::string, std::string> m{{"1","INTEGER"},{"2","VARCHAR"}};

    SECTION("Supporse a same type") {
        REQUIRE_FALSE(hasAnyTypeInternal(m, "1", "INTEGER"));
    }
    SECTION("Supporse a ANY type") {
        REQUIRE(hasAnyTypeInternal(m, "1", "VARCHAR"));
    }
}

static auto runBindStatementType(const std::string& sql, const std::vector<std::string>& schemas) -> duckdb::BoundStatement {
    auto db = duckdb::DuckDB(nullptr);
    auto conn = duckdb::Connection(db);

    prepare_schema: {
        for (auto& schema: schemas) {
            conn.Query(schema);
        }
    }

    auto stmts = conn.ExtractStatements(sql);

    duckdb::case_insensitive_map_t<duckdb::BoundParameterData> parameter_map{};
    duckdb::BoundParameterMap parameters(parameter_map);
    
    auto binder = duckdb::Binder::CreateBinder(*conn.context);
    binder->SetCanContainNulls(true);
    binder->parameters = &parameters;

    duckdb::BoundStatement bind_result;
    try {
        conn.BeginTransaction();
        bind_result = binder->Bind(*stmts[0]);
        conn.Commit();
    }
    catch (...) {
        conn.Rollback();
        throw;
    }

    return std::move(bind_result);
}

static auto runrResolveParamType(duckdb::BoundStatement& stmt, const ParamNameLookup& lookup, const ParamTypeLookup& param_types) -> void {
    auto resolve_result = resolveParamType(stmt.plan, lookup);

    SECTION("Parameter count") {
        REQUIRE(resolve_result.size() == lookup.size());
    }
    SECTION("Parameter entries") {
        for (int i = 0; auto& entry: resolve_result) {
            SECTION(std::format("valid nameed parameter#{}", i)) {
                REQUIRE(lookup.contains(entry.position));
                REQUIRE_THAT(entry.name, Equals(lookup.at(entry.position)));
            }

            if (param_types.contains(entry.position)) {
                SECTION(std::format("valid parameter type#{} (has type)", i)) {
                    REQUIRE(param_types.contains(entry.position));
                    REQUIRE_THAT(entry.type_name.value(), Equals(param_types.at(entry.position)));
                }
            }
            else {
                SECTION(std::format("valid parameter type#{} (has ANY type)", i)) {
                    REQUIRE_FALSE((bool)entry.type_name);
                }
            }
            ++i;
        }
    }
}

TEST_CASE("Resolve without parameters") {
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string sql("select * from Foo");
    ParamNameLookup lookup{};
    ParamTypeLookup types{};

    auto bound_statement = runBindStatementType(sql, {schema});
    runrResolveParamType(bound_statement, lookup, types);  
}

TEST_CASE("Resolve positional parameter on where clause") {
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string sql("select * from Foo where kind = $1");
    ParamNameLookup lookup{{"1","1"}};
    ParamTypeLookup types{{"1","INTEGER"}};

    auto bound_statement = runBindStatementType(sql, {schema});
    runrResolveParamType(bound_statement, lookup, types);
}

TEST_CASE("Resolve positional parameter on select list and where clause") {
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string sql("select id, $2::text from Foo where kind = $1");
    ParamNameLookup lookup{{"1","1"}, {"2","2"}};
    ParamTypeLookup types{{"1","INTEGER"}, {"2","VARCHAR"}};

    auto bound_statement = runBindStatementType(sql, {schema});
    runrResolveParamType(bound_statement, lookup, types);
}

TEST_CASE("Resolve named parameter on where clause") {
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string sql("select * from Foo where kind = $1");
    ParamNameLookup lookup{{"1","kind"}};
    ParamTypeLookup types{{"1","INTEGER"}};

    auto bound_statement = runBindStatementType(sql, {schema});
    runrResolveParamType(bound_statement, lookup, types);
}

TEST_CASE("Resolve named parameter on select list and where clause") {
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string sql("select id, $1::text from Foo where kind = $2");
    ParamNameLookup lookup{{"1","phrase"}, {"2","kind"}};
    ParamTypeLookup types{{"1","VARCHAR"}, {"2","INTEGER"}};

    auto bound_statement = runBindStatementType(sql, {schema});
    runrResolveParamType(bound_statement, lookup, types);   
}

TEST_CASE("Resolve named parameters on joined select list and where clause") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select Foo.id, $1::text 
        from Foo 
        join Bar on Foo.id = Bar.id and Bar.value <> $2
        where Foo.kind = $3
    )#");
    ParamNameLookup lookup{{"1","phrase"}, {"2", "serch_word"}, {"3","kind"}};
    ParamTypeLookup types{{"1","VARCHAR"}, {"2", "VARCHAR"}, {"3","INTEGER"}};

    auto bound_statement = runBindStatementType(sql, {schema_1, schema_2});
    runrResolveParamType(bound_statement, lookup, types);   
}

TEST_CASE("Resolve named parameter on select list and where clause with subquery") {
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string sql(R"#(
        select x.*, null, '' as "''", 1+2, $1::int as n
        from (
            select id, kind, 123::bigint, $2::text as s
            from Foo
            where kind = $3
        ) x
    )#");
    ParamNameLookup lookup{{"1","seq"}, {"2", "phrase"}, {"3","kind"}};
    ParamTypeLookup types{{"1","INTEGER"}, {"2", "VARCHAR"}, {"3","INTEGER"}};

    auto bound_statement = runBindStatementType(sql, {schema});
    runrResolveParamType(bound_statement, lookup, types);   
}

TEST_CASE("Resolve duplicated named parameter on select list and where clause with subquery#1 (same type)") {
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string sql(R"#(
        select x.*, null, '' as "''", 1+2, $1::int as n1
        from (
            select id, kind, 123::bigint, $2::int as c
            from Foo
            where kind = $2
        ) x
    )#");
    ParamNameLookup lookup{{"1","seq"}, {"2","kind"}};
    ParamTypeLookup types{{"1","INTEGER"}, {"2", "INTEGER"}};

    auto bound_statement = runBindStatementType(sql, {schema});
    runrResolveParamType(bound_statement, lookup, types);   
}

TEST_CASE("Resolve duplicated named parameter on select list and where clause with subquery#2 (NOT same type)") {
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string sql(R"#(
        select x.*, null, '' as "''", 1+2, $1::int as n1, $2::int as k2
        from (
            select id, kind, 123::bigint, $2::text || '_abc' as c
            from Foo
            where kind = $2::int
        ) x
    )#");
    ParamNameLookup lookup{{"1","seq"}, {"2","kind"}};
    ParamTypeLookup types{{"1","INTEGER"}};

    auto bound_statement = runBindStatementType(sql, {schema});
    runrResolveParamType(bound_statement, lookup, types);   
}

#endif
