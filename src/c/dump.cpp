#include <duckdb/parser/parser.hpp>

#include <duckdb/common/enums/expression_type.hpp>

#include <duckdb/parser/statement/select_statement.hpp>
#include <duckdb/parser/query_node/select_node.hpp>
#include <duckdb/parser/expression/star_expression.hpp>
#include <duckdb/parser/expression/cast_expression.hpp>
#include <duckdb/parser/expression/parameter_expression.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/comparison_expression.hpp>
#include <duckdb/parser/expression/conjunction_expression.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>

#define MAGIC_ENUM_RANGE_MAX (std::numeric_limits<uint8_t>::max())

#include <magic_enum/magic_enum_iostream.hpp>
#include <magic_enum/magic_enum.hpp>
#include <string>
#include <iostream>

using magic_enum::iostream_operators::operator<<;

auto dumpStrings(const duckdb::vector<std::string>& items) -> std::string {
    auto buf = std::string("");
    for (auto column: items) {
        buf.append(column); 
        buf.append(", ");
    }

    return buf;
}

auto dumpOptionalString(const std::string& item) -> std::string {
    return item != "" ? item : "(none)"; 
}

auto dumpQueryLocation(const duckdb::optional_idx& idx, uint32_t indent_level) -> void {
    if (idx.IsValid()) {
        std::cout << std::string(indent_level * 2, ' ') << "qry-loc: " << idx.GetIndex() << std::endl;
    } 
    else {
        std::cout << std::string(indent_level * 2, ' ') << "qry-loc: (INVALID)" << std::endl;
    }
}

auto dumpExpression(const duckdb::unique_ptr<duckdb::ParsedExpression>& expr, uint32_t indent_level) -> void;
auto dumpSelectStatement(const duckdb::SelectStatement& stmt, uint32_t indent_level) -> void;

auto dumpCastExpression(const duckdb::CastExpression& expr, uint32_t indent_level) -> void {
    std::cout << std::string(indent_level * 2, ' ') << "type: " << expr.cast_type.ToString() << std::endl;
    dumpExpression(expr.child, indent_level);
}

auto dumpParameterExpression(const duckdb::ParameterExpression& expr, uint32_t indent_level) -> void {
    std::cout << std::string(indent_level * 2, ' ') << "param-name: " << expr.identifier << std::endl;
}

auto dumpColumnRef(const duckdb::ColumnRefExpression& expr, uint32_t indent_level) -> void {
    std::cout << std::string(indent_level * 2, ' ') << "columns: " << dumpStrings(expr.column_names) << std::endl;
}

auto dumpFunctionExpr(const duckdb::FunctionExpression& expr, uint32_t indent_level) -> void {
    std::cout << std::string(indent_level * 2, ' ') << "catalog: " << dumpOptionalString(expr.catalog) << std::endl;
    std::cout << std::string(indent_level * 2, ' ') << "schema: " << dumpOptionalString(expr.schema) << std::endl;
    std::cout << std::string(indent_level * 2, ' ') << "function_name: " << dumpOptionalString(expr.function_name) << std::endl;
    std::cout << std::string(indent_level * 2, ' ') << "is_operator: " << expr.is_operator << std::endl;

    // expr.children
    std::cout << std::string(indent_level * 2, ' ') << "func-args" << std::endl;
    for (auto& arg: expr.children) {
        std::cout << std::string((indent_level+1) * 2, ' ') << "arg" << std::endl;
        dumpExpression(arg, indent_level+2);
    }
    
    std::cout << std::string(indent_level * 2, ' ') << "distinct: " << expr.distinct << std::endl;
    
    std::cout << std::string(indent_level * 2, ' ') << "filter: " << std::endl;
    if (expr.filter) {
        dumpExpression(expr.filter, indent_level+1);
    }
    else {
        std::cout << std::string((indent_level+1) * 2, ' ') << "(none)" << std::endl;
    }

    // expr.order_bys
    std::cout << std::string(indent_level * 2, ' ') << "[todo] func-orderby" << std::endl;

    std::cout << std::string(indent_level * 2, ' ') << "export_state: " << expr.export_state << std::endl;
}

auto dumpComparisonExpr(const duckdb::ComparisonExpression& expr, uint32_t indent_level) -> void {
    std::cout << std::string(indent_level * 2, ' ') << "expr-lhs" << std::endl;
    dumpExpression(expr.left, indent_level+1);
    std::cout << std::string(indent_level * 2, ' ') << "expr-rhs" << std::endl;
    dumpExpression(expr.right, indent_level+1);
}

auto dumpConjunctionExpr(const duckdb::ConjunctionExpression& expr, uint32_t indent_level) -> void {
    for (auto& item: expr.children) {
        std::cout << std::string(indent_level * 2, ' ') << "expr" << std::endl;
        dumpExpression(item, indent_level+1);
    }
}

auto dumpExpression(const duckdb::unique_ptr<duckdb::ParsedExpression>& expr, uint32_t indent_level) -> void {
    switch (expr->expression_class) {        
    case duckdb::ExpressionClass::CAST: 
        std::cout << std::string(indent_level * 2, ' ') << "expr: CastExpression" << std::endl;
        dumpCastExpression(expr->Cast<duckdb::CastExpression>(), indent_level+1);
        break;

    case duckdb::ExpressionClass::PARAMETER:
        std::cout << std::string(indent_level * 2, ' ') << "expr: ParameterExpression" << std::endl;
        dumpParameterExpression(expr->Cast<duckdb::ParameterExpression>(), indent_level+1);
        break;

    case duckdb::ExpressionClass::COLUMN_REF:
        std::cout << std::string(indent_level * 2, ' ') << "expr: ColumnRefExpression" << std::endl;
        dumpColumnRef(expr->Cast<duckdb::ColumnRefExpression>(), indent_level+1);
        break;
    
    case duckdb::ExpressionClass::FUNCTION:
        std::cout << std::string(indent_level * 2, ' ') << "expr: FunctionExpression" << std::endl;
        dumpFunctionExpr(expr->Cast<duckdb::FunctionExpression>(), indent_level+1);
        break;

    case duckdb::ExpressionClass::COMPARISON:
        std::cout << std::string(indent_level * 2, ' ') << "expr: ComparisonExpression" << std::endl;
        dumpComparisonExpr(expr->Cast<duckdb::ComparisonExpression>(), indent_level+1);
        break;

    case duckdb::ExpressionClass::CONJUNCTION:
        std::cout << std::string(indent_level * 2, ' ') << "expr: ConjunctionExpression" << std::endl;
        dumpConjunctionExpr(expr->Cast<duckdb::ConjunctionExpression>(), indent_level+1);
        break;

    default: 
        std::cout << std::string(indent_level * 2, ' ') << "[todo] Expression: " << magic_enum::enum_name(expr->expression_class) << std::endl;
        break;
    }

    std::cout << std::string(indent_level * 2, ' ') << "expr-type: " << expr->type << std::endl;
    std::cout << std::string(indent_level * 2, ' ') << "expr-alias: " << (expr->alias != "" ? expr->alias : "(none)") << std::endl;
    
    dumpQueryLocation(expr->query_location, indent_level);
}

auto dumpSelectList(const duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>>& nodes, uint32_t indent_level) -> void {
    std::cout << std::string(indent_level * 2, ' ') << "select-clause: " << nodes.size() << std::endl;

    for (auto& node: nodes) {
        dumpExpression(node, indent_level+1);
    }
}

auto dumpBaseTableRef(const duckdb::BaseTableRef& table, uint32_t indent_level) -> void {
    std::cout << std::string(indent_level * 2, ' ') << "catalog: " << dumpOptionalString(table.catalog_name) << std::endl;
    std::cout << std::string(indent_level * 2, ' ') << "schema: " << dumpOptionalString(table.schema_name) << std::endl;
    std::cout << std::string(indent_level * 2, ' ') << "table_name: " << dumpOptionalString(table.table_name) << std::endl;
    std::cout << std::string(indent_level * 2, ' ') << "column-alias: " << dumpStrings(table.column_name_alias) << std::endl;
}

auto dumpTableFunction(const duckdb::TableFunctionRef& table, uint32_t indent_level) -> void {
    std::cout << std::string(indent_level * 2, ' ') << "func-name: " << std::endl; 
    dumpExpression(table.function, indent_level+1);
    std::cout << std::string(indent_level * 2, ' ') << "table-column-alias: " << dumpStrings(table.column_name_alias) << std::endl; 
    std::cout << std::string(indent_level * 2, ' ') << "func-subquery: " << std::endl; 

    if (table.subquery) {
        dumpSelectStatement(*table.subquery, indent_level+1);
    }
    else {
        std::cout << std::string((indent_level+1) * 2, ' ') << "(none)" << std::endl; 
    }

    // external_dependency
    std::cout << std::string(indent_level * 2, ' ') << "[todo] external_dependency" << std::endl; 
}

auto dumpTableRef(const duckdb::unique_ptr<duckdb::TableRef>& table, uint32_t indent_level) -> void {
    std::cout << std::string(indent_level * 2, ' ') << "table-ref: " << std::endl;

    switch (table->type) {
    case duckdb::TableReferenceType::BASE_TABLE: 
        dumpBaseTableRef(table->Cast<duckdb::BaseTableRef>(), indent_level+1);
        break;

    case duckdb::TableReferenceType::TABLE_FUNCTION:
        dumpTableFunction(table->Cast<duckdb::TableFunctionRef>(), indent_level+1);
        break;
    // 
    // 
    // SUBQUERY
    // JOIN
    // 
    // EXPRESSION_LIST
    // CTE
    // EMPTY_FROM
    // PIVOT
    // SHOW_REF
    default: 
        std::cout << std::string((indent_level+1) * 2, ' ') << "[todo] table-type: " << table->type << std::endl;
        break;
    }

    std::cout << std::string((indent_level+1) * 2, ' ') << "table-alias: " << table->alias << std::endl;
    std::cout << std::string((indent_level+1) * 2, ' ') << "table-sample: " << table->sample << std::endl;

    dumpQueryLocation(table->query_location, indent_level+1);
}

auto dumpSelectNode(const duckdb::SelectNode& node, uint32_t indent_level) -> void {
    dumpSelectList(node.select_list, indent_level+1);
    dumpTableRef(node.from_table, indent_level+1);

    std::cout << std::string((indent_level+1) * 2, ' ') << "where-clause" << std::endl;
    dumpExpression(node.where_clause, indent_level+2);


    // sel_node.groups
    // sel_node.having
    // sel_node.qualify
    // sel_node.aggregate_handling
    // sel_node.sample

    // std::cout << "\t" << "cte: " << std::endl;

    // for (auto& it: sel_stmt.node->cte_map.map) {
    //     std::cout << "\t\t" << "name: " << it.first << "materialized: " << it.second->materialized << std::endl;
    //     std::cout << "\t\t\t" << "(CTE query is recursive `SelectStatement` type)" << std::endl;
    // }

    // std::cout << "\t" << "modifier: " << sel_stmt.node->modifiers.size() << std::endl;

    // for (auto& modifier: node->modifiers) {
    //     // LIMIT_MODIFIER
    //     // ORDER_MODIFIER
    //     // DISTINCT_MODIFIER
    //     // LIMIT_PERCENT_MODIFIER
    // }
}

auto dumpSelectStatement(const duckdb::SelectStatement& stmt, uint32_t indent_level) -> void {
    switch (stmt.node->type) {
        case duckdb::QueryNodeType::SELECT_NODE: {
            dumpSelectNode(stmt.node->Cast<duckdb::SelectNode>(), indent_level);
            break;
        }
        default: {
            std::cout << std::string(indent_level * 2, ' ') << "[todo] node-type: " << stmt.node->type << std::endl;
            break;
        }
    }
}

extern "C" {

auto dmpParseedSQL(const char *query, uint32_t len) -> void {
    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query, len));

    std::cout << "statements: " << parser.statements.size() << std::endl;

    for (const auto& stmt: parser.statements) {
        switch (stmt->type) {
        case duckdb::StatementType::SELECT_STATEMENT: 
            dumpSelectStatement(stmt->Cast<duckdb::SelectStatement>(), 1);
            break;

        // INSERT_STATEMENT
        // UPDATE_STATEMENT
        // DELETE_STATEMENT
        // COPY_STATEMENT
        default:
            std::cout << "[todo] statement-type:" << stmt->type << std::endl;
            break;
        }
    }
}

}