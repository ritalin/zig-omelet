#pragma once

#include <duckdb.hpp>

#include "duckdb_binder_support.hpp"
#include "zmq_worker_support.hpp"

namespace worker {

class JoinTypeVisitor: public duckdb::LogicalOperatorVisitor {
public:
    using Rel = duckdb::idx_t;
    using ConditionRels = std::map<ColumnNullableLookup::Column, ColumnNullableLookup::Column>;
public:
     JoinTypeVisitor(ColumnNullableLookup& lookup, ColumnNullableLookup& parent_lookup_ref, SampleNullableCache& sample_cache_ref, duckdb::Connection& conn_ref, ZmqChannel& channel): 
        channel(channel), conn(conn_ref), join_type_lookup(lookup), parent_lookup(parent_lookup_ref), sample_cache(sample_cache_ref)
    {
    }
public:
    auto VisitOperator(duckdb::LogicalOperator &op) -> void;
private:
    auto VisitOperatorGet(const duckdb::LogicalGet& op) -> void;
    auto VisitOperatorValuesGet(duckdb::LogicalExpressionGet& op) -> void;
    auto VisitOperatorGroupBy(duckdb::LogicalAggregate& op) -> void;
    auto VisitOperatorCteRef(duckdb::LogicalCTERef& op) -> void;
    auto VisitOperatorJoin(duckdb::LogicalJoin& op, ConditionRels&& rels) -> void;
    auto VisitOperatorJoinInternal(duckdb::LogicalOperator &op, duckdb::JoinType ty_left, duckdb::JoinType ty_right, ConditionRels&& rels) -> void;
    auto VisitOperatorCondition(duckdb::LogicalOperator &op, duckdb::JoinType ty_left, const ConditionRels& rels) -> ColumnNullableLookup;
private:
    ZmqChannel& channel;
    duckdb::Connection& conn;
    ColumnNullableLookup& join_type_lookup;
    ColumnNullableLookup& parent_lookup;
    SampleNullableCache& sample_cache;
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
    ColumnExpressionVisitor(const ColumnNullableLookup::Column& binding_to, ColumnNullableLookup& field_nullabilities, SampleNullableCache& sample_cache_ref): 
        binding_to(binding_to), nullabilities(field_nullabilities), sample_cache(sample_cache_ref)
    {
    }
public:
    static auto Resolve(duckdb::unique_ptr<duckdb::Expression> &expr, const ColumnNullableLookup::Column& binding_to, ColumnNullableLookup& nullabilitiess, SampleNullableCache& sample_cache) -> ColumnNullableLookup::Item;
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
    ColumnNullableLookup::Column binding_to;
    ColumnNullableLookup& nullabilities;
    std::stack<bool> nullable_stack;
    SampleNullableCache& sample_cache;
};

}