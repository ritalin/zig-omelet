#pragma once

#include <duckdb.hpp>

#include "duckdb_binder_support.hpp"

namespace worker {

class JoinTypeVisitor: public duckdb::LogicalOperatorVisitor {
public:
    using Rel = duckdb::idx_t;
    using ConditionRels = std::map<NullableLookup::Column, NullableLookup::Column>;
public:
     JoinTypeVisitor(NullableLookup& lookup, const CatalogLookup& catalogs): join_type_lookup(lookup), catalogs(catalogs) {
     }
public:
    auto VisitOperator(duckdb::LogicalOperator &op) -> void;
private:
    auto VisitOperatorGet(const duckdb::LogicalGet& op) -> void;
    auto VisitOperatorJoin(duckdb::LogicalJoin& op, ConditionRels&& rels) -> void;
    auto VisitOperatorJoinInternal(duckdb::LogicalOperator &op, duckdb::JoinType ty_left, duckdb::JoinType ty_right, ConditionRels&& rels) -> void;
    // auto VisitOperatorCondition(duckdb::LogicalOperator &op, duckdb::JoinType ty_left, const ConditionRels& rels) -> std::vector<JoinTypePair>;
    auto VisitOperatorCondition(duckdb::LogicalOperator &op, duckdb::JoinType ty_left, const ConditionRels& rels) -> NullableLookup;
private:
    CatalogLookup catalogs;
    NullableLookup& join_type_lookup;
};

// ================================================================================

class TableCatalogResolveVisitor {
public:
    TableCatalogResolveVisitor(CatalogLookup& lookup_ref): lookup(lookup_ref) {}
public:
    auto VisitTableRef(duckdb::unique_ptr<duckdb::BoundTableRef> &table_ref) -> void;
    auto VisitSelectNode(duckdb::unique_ptr<duckdb::BoundQueryNode>& node) ->void;
private:
    CatalogLookup& lookup;
};

// ================================================================================

// struct ColumnBindingNode;
// using NodeRef = std::shared_ptr<ColumnBindingNode>;

// enum class NodeKind { FromTable, Consume };
// struct ColumnBindingNode{
//     NodeRef next;
//     NodeKind kind;
//     duckdb::idx_t table_index;
//     duckdb::idx_t column_index;
//     bool nullable;
// };

class ColumnExpressionVisitor: public duckdb::LogicalOperatorVisitor {
public:
    static auto Resolve(duckdb::unique_ptr<duckdb::Expression> &expr) -> NullableLookup::Nullability;
public:
    // auto VisitOperator(duckdb::LogicalOperator &op) -> void;
protected:
    // auto VisitReplace(duckdb::BoundColumnRefExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundConstantExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundFunctionExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundOperatorExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundComparisonExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundParameterExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
	auto VisitReplace(duckdb::BoundCaseExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundSubqueryExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
private:
    // auto VisitColumnBinding(duckdb::unique_ptr<duckdb::Expression>&& expr, duckdb::ColumnBinding&& binding) -> void;
    auto EvalNullability(duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& expressions, bool terminate_value) -> bool;
    auto EvalNullabilityInternal(duckdb::unique_ptr<duckdb::Expression>& expr) -> bool;
    // auto RegisterBindingLink(const std::optional<duckdb::ColumnBinding>& current_binding, bool nullable) -> void;
private:
    CatalogLookup catalogs;
    // NullableLookup::Nullability& nullable;
    std::optional<duckdb::ColumnBinding> current_binding;
    std::stack<bool> nullable_stack;
};

}