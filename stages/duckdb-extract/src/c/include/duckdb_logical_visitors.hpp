#pragma once

#include <duckdb.hpp>

#include "duckdb_binder_support.hpp"

namespace worker {

class JoinTypeVisitor: public duckdb::LogicalOperatorVisitor {
public:
    using Rel = duckdb::idx_t;
    using ConditionRels = std::map<NullableLookup::Column, NullableLookup::Column>;
public:
     JoinTypeVisitor(NullableLookup& lookup): join_type_lookup(lookup) {
     }
public:
    auto VisitOperator(duckdb::LogicalOperator &op) -> void;
private:
    auto VisitOperatorGet(const duckdb::LogicalGet& op) -> void;
    auto VisitOperatorJoin(duckdb::LogicalJoin& op, ConditionRels&& rels) -> void;
    auto VisitOperatorJoinInternal(duckdb::LogicalOperator &op, duckdb::JoinType ty_left, duckdb::JoinType ty_right, ConditionRels&& rels) -> void;
    auto VisitOperatorCondition(duckdb::LogicalOperator &op, duckdb::JoinType ty_left, const ConditionRels& rels) -> NullableLookup;
private:
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

class ColumnExpressionVisitor: public duckdb::LogicalOperatorVisitor {
public:
    ColumnExpressionVisitor(NullableLookup& field_nullabilities): nullabilities(field_nullabilities) {}
public:
    static auto Resolve(duckdb::unique_ptr<duckdb::Expression> &expr, NullableLookup& nullabilities) -> NullableLookup::Nullability;
protected:
    auto VisitReplace(duckdb::BoundColumnRefExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundConstantExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundFunctionExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundOperatorExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundComparisonExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundParameterExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
	auto VisitReplace(duckdb::BoundCaseExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundSubqueryExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
private:
    auto EvalNullability(duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& expressions, bool terminate_value) -> bool;
    auto EvalNullabilityInternal(duckdb::unique_ptr<duckdb::Expression>& expr) -> bool;
private:
    NullableLookup& nullabilities;
    std::stack<bool> nullable_stack;
};

}