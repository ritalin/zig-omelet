#include <duckdb.hpp>
#include <duckdb/parser/statement/insert_statement.hpp>

#include  "statement_walker_support.hpp"

namespace worker {

auto ParameterCollector::walkInsertStatement(duckdb::InsertStatement& stmt) -> ParameterCollector::Result {
    handle_cte: {
        walkCTEStatement(*this, stmt.cte_map);
    }
    handle_select_stmt: if (stmt.select_statement) {
        walkSelectStatementInternal(*this, *stmt.select_statement, 0);
    }
    handle_conflict: if (stmt.on_conflict_info) {
        index_condition: if (stmt.on_conflict_info->condition) {
	        walkExpression(*this, stmt.on_conflict_info->condition, 0);
        }
        handle_update: if (stmt.on_conflict_info->set_info) {
            for (auto& expr: stmt.on_conflict_info->set_info->expressions) {
                walkExpression(*this, expr, 0);
            }
        }
        handle_update_condition: if (stmt.on_conflict_info->set_info && stmt.on_conflict_info->set_info->condition) {
	        walkExpression(*this, stmt.on_conflict_info->set_info->condition, 0);
        }
    }
    handle_returning: {
        for (auto& expr: stmt.returning_list) {
            walkReturningList(*this, stmt.returning_list);
        }         
    }

    return {
        .type = StatementType::Insert, 
        .names = swapMapEntry(this->name_map), 
        .examples = std::move(this->examples),
    };
}

}