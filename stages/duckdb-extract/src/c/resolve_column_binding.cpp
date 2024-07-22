#include <duckdb.hpp>
#include <duckdb/planner/operator/logical_get.hpp>


#include "duckdb_binder_support.hpp"

namespace worker {

class ColumnBindingVisitor: public duckdb::LogicalOperatorVisitor {
public:
    ColumnBindingVisitor(std::vector<ColumnBindingPair>& lookup): lookup(lookup) {}
public:
    auto VisitOperator(duckdb::LogicalOperator &op) -> void;
private:
    std::vector<ColumnBindingPair>& lookup;
};

static auto VisitOperatorGet(std::vector<ColumnBindingPair>& lookup, const duckdb::LogicalGet& op) -> void {
    for (size_t i = 0; auto id: op.column_ids) {
        lookup.push_back({
            .from = duckdb::ColumnBinding{op.table_index, i}, 
            .to = duckdb::ColumnBinding{op.table_index, id}
        });
        ++i;
    }
}

auto ColumnBindingVisitor::VisitOperator(duckdb::LogicalOperator &op) -> void {
    if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
        VisitOperatorGet(this->lookup, op.Cast<duckdb::LogicalGet>());
    }
    
    duckdb::LogicalOperatorVisitor::VisitOperator(op);
}

auto createColumnBindingLookup(std::vector<ColumnBindingPair>& lookup, duckdb::unique_ptr<duckdb::LogicalOperator>& op) -> void {
    ColumnBindingVisitor visitor(lookup);
    visitor.VisitOperator(*op);
}

}