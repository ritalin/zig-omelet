

#include <duckdb.hpp>
#include <duckdb/parser/expression/list.hpp>
#include <duckdb/planner/tableref/list.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/query_node/bound_select_node.hpp>
#include <duckdb/planner/bound_parameter_map.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>

#include "duckdb_logical_visitors.hpp"
#include "duckdb_binder_support.hpp"

#include <iostream>
#include <magic_enum/magic_enum.hpp>

namespace worker {

class TableCatalogResolveVisitor {
public:
    TableCatalogResolveVisitor(CatalogLookup& lookup_ref, ZmqChannel& channel): channel(channel), lookup(lookup_ref) {}
public:
    auto VisitTableRef(duckdb::unique_ptr<duckdb::BoundTableRef> &table_ref) -> void;
    auto VisitSelectNode(duckdb::unique_ptr<duckdb::BoundQueryNode>& node) ->void;
private:
    ZmqChannel& channel;
    CatalogLookup& lookup;
};

auto bindTypeToTableRefInternal(duckdb::shared_ptr<duckdb::Binder>& binder, duckdb::SelectNode& node) -> std::vector<duckdb::unique_ptr<duckdb::BoundTableRef>>;

auto walkScalarSubquery(duckdb::shared_ptr<duckdb::Binder>& binder, duckdb::unique_ptr<duckdb::ParsedExpression>& expr, std::vector<duckdb::unique_ptr<duckdb::BoundTableRef>>& results) -> void {
    if (expr->expression_class != duckdb::ExpressionClass::SUBQUERY) return;

    auto& sq = expr->Cast<duckdb::SubqueryExpression>();

    if (sq.subquery->node->type == duckdb::QueryNodeType::SELECT_NODE) {
        auto internal_results = bindTypeToTableRefInternal(binder, sq.subquery->node->Cast<duckdb::SelectNode>());
        results.insert(results.cend(), std::make_move_iterator(internal_results.begin()), std::make_move_iterator(internal_results.end()));
    }
}

auto bindTypeToTableRefInternal(duckdb::shared_ptr<duckdb::Binder>& binder, duckdb::SelectNode& node) -> std::vector<duckdb::unique_ptr<duckdb::BoundTableRef>> {
    std::vector<duckdb::unique_ptr<duckdb::BoundTableRef>> results{};
    
    results.push_back(binder->Bind(*node.from_table));

    for (auto& expr: node.select_list) {
        walkScalarSubquery(binder, expr, results);
    }

    return std::move(results);
}

auto bindTypeToTableRef(duckdb::ClientContext& context, duckdb::unique_ptr<duckdb::SQLStatement>&& stmt, StatementType type) -> std::vector<duckdb::unique_ptr<duckdb::BoundTableRef>> {
    if (type != StatementType::Select) {
        return {};
    }

    auto& select_stmt = stmt->Cast<duckdb::SelectStatement>();

    if (select_stmt.node->type != duckdb::QueryNodeType::SELECT_NODE) {
        return {};
    }

    duckdb::case_insensitive_map_t<duckdb::BoundParameterData> parameter_map{};
    duckdb::BoundParameterMap parameters(parameter_map);
    
    auto binder = duckdb::Binder::CreateBinder(context);

    binder->SetCanContainNulls(true);
    binder->parameters = &parameters;

    return bindTypeToTableRefInternal(binder, select_stmt.node->Cast<duckdb::SelectNode>());
}

auto TableCatalogResolveVisitor::VisitSelectNode(duckdb::unique_ptr<duckdb::BoundQueryNode>& node) ->void {
    switch (node->type) {
    case duckdb::QueryNodeType::SELECT_NODE:
        {
            auto& select_node = node->Cast<duckdb::BoundSelectNode>();
            this->VisitTableRef(select_node.from_table);
        }
        break;
    default:
        // TODO: warning
        this->channel("[TODO] Unimplemented bound query node: {}", magic_enum::enum_name(node->type));
        break;
    }
}

auto TableCatalogResolveVisitor::VisitTableRef(duckdb::unique_ptr<duckdb::BoundTableRef>& table_ref) -> void {
    switch (table_ref->type) {
    case duckdb::TableReferenceType::BASE_TABLE:
        {
            auto& table = table_ref->Cast<duckdb::BoundBaseTableRef>();
            auto& get = table.get->Cast<duckdb::LogicalGet>();

            this->lookup.insert(std::make_pair<duckdb::idx_t, duckdb::TableCatalogEntry*>(
                std::move(get.table_index), 
                std::move(&table.table)
            ));
        }
        break;
    case duckdb::TableReferenceType::SUBQUERY:
        {
            auto& sq = table_ref->Cast<duckdb::BoundSubqueryRef>();
            this->VisitSelectNode(sq.subquery);
        }
        break;
    case duckdb::TableReferenceType::JOIN:
        {
            auto& table = table_ref->Cast<duckdb::BoundJoinRef>();
            this->VisitTableRef(table.left);
            this->VisitTableRef(table.right);
        }
        break;
    case duckdb::TableReferenceType::EMPTY_FROM:
        // empty
        break;
    default:
        // TODO: warning
        std::cout << std::format("[TODO] Unimplemented bound table ref: {}", magic_enum::enum_name(table_ref->type)) << std::endl;
        break;
    }
}

auto resolveTableCatalog(std::vector<duckdb::unique_ptr<duckdb::BoundTableRef>>& table_references) -> CatalogLookup {
    CatalogLookup catalogs{};

    for (auto& table_ref: table_references) {
        TableCatalogResolveVisitor visitor(catalogs);
        visitor.VisitTableRef(table_ref);
    }

    return catalogs;
}

}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include "duckdb_binder_support.hpp"

using namespace worker;
using namespace Catch::Matchers;

struct CayalogExpect {
    duckdb::idx_t table_index;
    std::string name;
};

auto runPickCatalog(const std::string sql, const std::vector<std::string>& schemas, std::vector<CayalogExpect> expects) -> void {
    auto db = duckdb::DuckDB(nullptr);
    auto conn = duckdb::Connection(db);

    prepare_schema: {
        for (auto& schema: schemas) {
            conn.Query(schema);
        }
    }

    CatalogLookup results{};
    try {
        conn.BeginTransaction();

        auto stmts = conn.ExtractStatements(sql);
        auto stmt_type = evalStatementType(stmts[0]);

        auto bound_table_refs = bindTypeToTableRef(*conn.context, std::move(stmts[0]->Copy()), stmt_type);

        results = resolveTableCatalog(bound_table_refs);
        conn.Commit();
    }
    catch (...) {
        conn.Rollback();
        throw;
    }

    result_size: {
        UNSCOPED_INFO("Result size");
        REQUIRE(results.size() == expects.size());
    }
    result_entries: {
        for (int i = 1; auto [table_index, name]: expects) {
            INFO(std::format("Catalog#{}", i));
            
            has_catalog: {
                UNSCOPED_INFO("Has table_index");
                REQUIRE(results.contains(table_index));
            }
            catalog_name: {
                UNSCOPED_INFO("Catalog name");
                REQUIRE(results.at(table_index)->name == name);
            }
            ++i;
        }
    }
}

TEST_CASE("Catalog#1 (single table)") {
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string sql("select * from Foo");

    std::vector<CayalogExpect> expects{
        {.table_index = 0, .name = "Foo"},
    };

    runPickCatalog(sql, {schema}, expects);
}

TEST_CASE("Catalog#2 (joined table)") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select * from Foo
        join Bar on Foo.id = Bar.id
    )#");

    std::vector<CayalogExpect> expects{
        {.table_index = 0, .name = "Foo"},
        {.table_index = 1, .name = "Bar"},
    };

    runPickCatalog(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Catalog#2 (derived table)") {
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string sql(R"#(
        select v.* from (
            select * from Foo
        ) v
    )#");

    std::vector<CayalogExpect> expects{
        {.table_index = 0, .name = "Foo"},
    };

    runPickCatalog(sql, {schema}, expects);
}

TEST_CASE("Catalog#3 (with scalar subquery)") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select (select value from Bar where bar.id = Foo.id) as x
        from Foo
    )#");

    std::vector<CayalogExpect> expects{
        {.table_index = 0, .name = "Foo"},
        {.table_index = 1, .name = "Bar"},
    };

    runPickCatalog(sql, {schema_1, schema_2}, expects);
}

#endif
