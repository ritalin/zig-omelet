#include <duckdb.hpp>
#include <duckdb/parser/statement/update_statement.hpp>

#include  "statement_walker_support.hpp"

namespace worker {

auto ParameterCollector::walkUpdateStatement(duckdb::UpdateStatement& stmt) -> ParameterCollector::Result {
    handle_CTE: {
        walkCTEStatement(*this, stmt.cte_map);
    }
    handle_set: if (stmt.set_info) {
        for (auto& expr: stmt.set_info->expressions) {
            walkExpression(*this, expr, 0);
        }
    }
    handle_table_ref: if (stmt.from_table) {
        walkTableRef(*this, stmt.from_table, 0);
    }
    handle_where: if (stmt.set_info && stmt.set_info->condition) {
        walkExpression(*this, stmt.set_info->condition, 0);
    }
    handle_returning: {
        for (auto& expr: stmt.returning_list) {
            walkReturningList(*this, stmt.returning_list);
        }         
    }

    return {
        .type = StatementType::Update, 
        .names = swapMapEntry(this->name_map), 
        .examples = std::move(this->examples),
    };
}

}
