#include <duckdb.hpp>
#include <duckdb/parser/statement/delete_statement.hpp>

#include  "statement_walker_support.hpp"

namespace worker {

auto ParameterCollector::walkDeleteStatement(duckdb::DeleteStatement& stmt) -> ParameterCollector::Result {
    handle_CTE: {
        walkCTEStatement(*this, stmt.cte_map);
    }
    handle_using: for (auto& table: stmt.using_clauses) {
        walkTableRef(*this, table, 0);
    }
    handle_where: if (stmt.condition) {
        walkExpression(*this, stmt.condition, 0);
    }
    handle_returning: {
        for (auto& expr: stmt.returning_list) {
            walkReturningList(*this, stmt.returning_list);
        }         
    }

    return {
        .type = StatementType::Delete, 
        .names = swapMapEntry(this->name_map), 
        .examples = std::move(this->examples),
    };
}

}
