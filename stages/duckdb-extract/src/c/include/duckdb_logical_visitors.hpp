#pragma once

#include <duckdb.hpp>

#include "duckdb_binder_support.hpp"
#include "zmq_worker_support.hpp"

namespace worker {

class JoinTypeVisitor: public duckdb::LogicalOperatorVisitor {
public:
    using Rel = duckdb::idx_t;
    using ConditionRels = std::map<NullableLookup::Column, NullableLookup::Column>;
public:
     JoinTypeVisitor(NullableLookup& lookup, NullableLookup& parent_lookup_ref, const CteColumnBindingsRef& cte_columns, ZmqChannel& channel): 
        channel(channel), join_type_lookup(lookup), parent_lookup(parent_lookup_ref), cte_columns(cte_columns) 
    {
    }
public:
    auto VisitOperator(duckdb::LogicalOperator &op) -> void;
private:
    auto VisitOperatorGet(const duckdb::LogicalGet& op) -> void;
    auto VisitOperatorGroupBy(duckdb::LogicalAggregate& op) -> void;
    auto VisitOperatorCteRef(duckdb::LogicalCTERef& op) -> void;
    auto VisitOperatorJoin(duckdb::LogicalJoin& op, ConditionRels&& rels) -> void;
    auto VisitOperatorJoinInternal(duckdb::LogicalOperator &op, duckdb::JoinType ty_left, duckdb::JoinType ty_right, ConditionRels&& rels) -> void;
    auto VisitOperatorCondition(duckdb::LogicalOperator &op, duckdb::JoinType ty_left, const ConditionRels& rels) -> NullableLookup;
private:
    ZmqChannel& channel;
    NullableLookup& join_type_lookup;
    NullableLookup& parent_lookup;
    CteColumnBindingsRef cte_columns;
};

class ColumnNameVisitor: public duckdb::LogicalOperatorVisitor {
public:
    static auto Resolve(duckdb::unique_ptr<duckdb::Expression>& expr) -> std::string;
protected:
    auto VisitReplace(duckdb::BoundConstantExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
private:
    std::string column_name;
};

// ================================================================================

class ColumnExpressionVisitor: public duckdb::LogicalOperatorVisitor {
public:
    ColumnExpressionVisitor(NullableLookup& field_nullabilities): nullabilities(field_nullabilities) {}
public:
    static auto Resolve(duckdb::unique_ptr<duckdb::Expression> &expr, NullableLookup& nullabilitiess) -> NullableLookup::Nullability;
protected:
    auto VisitReplace(duckdb::BoundColumnRefExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundConstantExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundFunctionExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
	auto VisitReplace(duckdb::BoundAggregateExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundOperatorExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundComparisonExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundParameterExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
	auto VisitReplace(duckdb::BoundCaseExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundSubqueryExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundWindowExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundUnnestExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
private:
    auto EvalNullability(duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& expressions, bool terminate_value) -> bool;
    auto EvalNullabilityInternal(duckdb::unique_ptr<duckdb::Expression>& expr) -> bool;
private:
    NullableLookup& nullabilities;
    std::stack<bool> nullable_stack;
};

}