#include <ranges>

#include <duckdb.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_comparison_join.hpp>
#include <duckdb/planner/operator/logical_any_join.hpp>


#include "duckdb_binder_support.hpp"

#include <iostream>

namespace worker {

class JoinTypeVisitor: public duckdb::LogicalOperatorVisitor {
public:
    using Rel = duckdb::idx_t;
    using ConditionRels = std::vector<duckdb::idx_t>;
public:
     JoinTypeVisitor(std::vector<JoinTypePair>& lookup): join_type_lookup(lookup) {
     }
public:
    auto VisitOperator(duckdb::LogicalOperator &op) -> void;
private:
    auto VisitOperatorGet(const duckdb::LogicalGet& op) -> void;
    auto VisitOperatorJoin(duckdb::LogicalJoin& op, ConditionRels&& rels) -> void;
    auto VisitOperatorJoinInternal(duckdb::LogicalOperator &op, duckdb::JoinType ty_left, duckdb::JoinType ty_right, ConditionRels&& rels) -> void;
    auto VisitOperatorCondition(duckdb::LogicalOperator &op, duckdb::JoinType ty_left, const ConditionRels& rels) -> std::vector<JoinTypePair>;
private:
    std::vector<JoinTypePair>& join_type_lookup;
};

static auto EvaluateNullability(duckdb::JoinType rel_join_type, JoinTypePair col_join_pair, const JoinTypeVisitor::ConditionRels& conditions, const std::vector<JoinTypePair>& join_types) -> duckdb::JoinType {
    if (rel_join_type == duckdb::JoinType::OUTER) {
        return rel_join_type;
    }
    if (col_join_pair.join_type == duckdb::JoinType::OUTER) {
        return col_join_pair.join_type;
    }

    auto join_type_view = 
        join_types
        | std::views::filter([](const JoinTypePair& p) { return p.join_type == duckdb::JoinType::OUTER; })
        | std::views::transform([](const JoinTypePair& p) { return p.binding.table_index; })
    ;
    std::unordered_multiset<JoinTypeVisitor::Rel> lookup(join_type_view.begin(), join_type_view.end());

    auto view = 
        conditions
        | std::views::filter([&](JoinTypeVisitor::Rel r) { return r != col_join_pair.binding.table_index; })
        | std::views::filter([&](JoinTypeVisitor::Rel r) { return lookup.contains(r); })
    ;

    return view.empty() ? duckdb::JoinType::INNER : duckdb::JoinType::OUTER;
}

auto JoinTypeVisitor::VisitOperatorGet(const duckdb::LogicalGet& op) -> void {
    this->join_type_lookup.reserve(this->join_type_lookup.size() + op.column_ids.size());

    for (auto& col: op.column_ids) {
        this->join_type_lookup.emplace_back(JoinTypePair {
            .binding = duckdb::ColumnBinding(op.table_index, col),
            .join_type = duckdb::JoinType::INNER,
        });
    }
}

auto JoinTypeVisitor::VisitOperatorCondition(duckdb::LogicalOperator &op, duckdb::JoinType join_type, const ConditionRels& rels) -> std::vector<JoinTypePair> {
    std::vector<JoinTypePair> internal_join_types{};
    JoinTypeVisitor visitor(internal_join_types);

    visitor.VisitOperator(op);

    this->join_type_lookup.reserve(this->join_type_lookup.size() + internal_join_types.size());

    for (auto& p: internal_join_types) {
        this->join_type_lookup.emplace_back(JoinTypePair {
            .binding = std::move(p.binding),
            .join_type = EvaluateNullability(join_type, p, rels, this->join_type_lookup),
        });
    }

    return std::move(internal_join_types);
}


auto JoinTypeVisitor::VisitOperatorJoinInternal(duckdb::LogicalOperator &op, duckdb::JoinType ty_left, duckdb::JoinType ty_right, ConditionRels&& rels) -> void {
    resolve_join_type: {
        // walk in left
        auto left_cols = this->VisitOperatorCondition(*op.children[0], ty_left, {});
        // walk in right
        auto right_cols = this->VisitOperatorCondition(*op.children[1], ty_right, rels);
    }
}

namespace binding {
    class ConditionBindingVisitor: public duckdb::LogicalOperatorVisitor {
    public:
        ConditionBindingVisitor(std::unordered_set<JoinTypeVisitor::Rel>& rels): condition_rels(rels) {}
    protected:
        auto VisitReplace(duckdb::BoundColumnRefExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
            this->condition_rels.insert(expr.binding.table_index);
            return nullptr;
        }
    private:
        std::unordered_set<JoinTypeVisitor::Rel>& condition_rels;
    };
}

static auto VisitJoinCondition(duckdb::vector<duckdb::JoinCondition>& conditions) -> JoinTypeVisitor::ConditionRels {
    std::unordered_set<JoinTypeVisitor::Rel> rels{};
    binding::ConditionBindingVisitor visitor(rels);

    for (auto& c: conditions) {
        visitor.VisitExpression(&c.left);
        visitor.VisitExpression(&c.right);
    }

    return std::move(JoinTypeVisitor::ConditionRels(rels.begin(), rels.end()));
}

static auto VisitDelimJoinCondition(duckdb::LogicalComparisonJoin& op) -> JoinTypeVisitor::ConditionRels {
    std::unordered_set<JoinTypeVisitor::Rel> rels{};
    binding::ConditionBindingVisitor visitor(rels);

    for (auto& c: op.children) {
        visitor.VisitOperator(*c);
    }
    for (auto& c: op.conditions) {
        visitor.VisitExpression(&c.left);
        visitor.VisitExpression(&c.right);
    }

    return std::move(JoinTypeVisitor::ConditionRels(rels.begin(), rels.end()));
}

static auto ToOuterAll(std::vector<JoinTypePair>& lookup) -> void {
    for (auto& p: lookup) {
        p.join_type = duckdb::JoinType::OUTER;
    }
}

auto JoinTypeVisitor::VisitOperatorJoin(duckdb::LogicalJoin& op, ConditionRels&& rels) -> void {
    if (op.join_type == duckdb::JoinType::INNER) {
        this->VisitOperatorJoinInternal(op, duckdb::JoinType::INNER, duckdb::JoinType::INNER, std::move(rels));
    }
    else if (op.join_type == duckdb::JoinType::LEFT) {
        this->VisitOperatorJoinInternal(op, duckdb::JoinType::INNER, duckdb::JoinType::OUTER, std::move(rels));
    }
    else if (op.join_type == duckdb::JoinType::RIGHT) {
        this->VisitOperatorJoinInternal(op, duckdb::JoinType::OUTER, duckdb::JoinType::INNER, std::move(rels));
    }
    else if (op.join_type == duckdb::JoinType::OUTER) {
        this->VisitOperatorJoinInternal(op, duckdb::JoinType::OUTER, duckdb::JoinType::OUTER, {});
        ToOuterAll(this->join_type_lookup);
    }
}

static auto debugDumpJoinTypeBindings(const std::vector<JoinTypePair>& join_types) -> std::string {
    std::vector<std::string> result;
    result.reserve(join_types.size() * 2);

    for (auto& p: join_types) {
        result.emplace_back(std::format("({}, {})", p.binding.table_index, p.binding.column_index));
        result.emplace_back(", ");
    }

    auto view = result | std::views::join;

    return std::move(std::string(view.begin(), view.end()));
}

static auto fildJoinType(const std::vector<JoinTypePair>& join_types, duckdb::ColumnBinding binding) -> duckdb::JoinType {
    auto iter = std::ranges::find_if(join_types, [&](const JoinTypePair& p) { 
        return p.binding == binding; 
    });

    // if (iter == join_types.end()) {
    //     std::cout << std::format("binding: ({},{})", binding.table_index, binding.column_index) << std::endl;
    //     std::cout << std::format("join_types/size: {}", join_types.size()) << std::endl;
    //     std::cout << std::format("join_types/bindings: {}", debugDumpJoinTypeBindings(join_types)) << std::endl;
    // }

    return iter->join_type;
}

static auto VisitOperatorProjection(duckdb::LogicalProjection& op) -> std::vector<JoinTypePair> {
    std::vector<JoinTypePair> internal_join_types{};
    JoinTypeVisitor visitor(internal_join_types);

    for (auto& child: op.children) {
        visitor.VisitOperator(*child);
    }

    std::vector<JoinTypePair> results;
    results.reserve(op.expressions.size());

    if (internal_join_types.empty()) {
        return results;
    }

    for (duckdb::idx_t i = 0; auto& expr: op.expressions) {
        duckdb::ColumnBinding binding(op.table_index, i);

        if (expr->expression_class == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
            auto& expr_column = expr->Cast<duckdb::BoundColumnRefExpression>();
            results.emplace_back(JoinTypePair {.binding = binding, .join_type = fildJoinType(internal_join_types, expr_column.binding)});
        }
        else {
            results.emplace_back(JoinTypePair {.binding = binding, .join_type = duckdb::JoinType::INNER});
        }
        ++i;
    }

    return std::move(results);
}

auto JoinTypeVisitor::VisitOperator(duckdb::LogicalOperator &op) -> void {
    switch (op.type) {
    case duckdb::LogicalOperatorType::LOGICAL_PROJECTION:
        {
            auto lookup = VisitOperatorProjection(op.Cast<duckdb::LogicalProjection>());
            this->join_type_lookup.insert(this->join_type_lookup.end(), lookup.begin(), lookup.end());
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_GET:
        this->VisitOperatorGet(op.Cast<duckdb::LogicalGet>());
        break;
    case duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
    case duckdb::LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        {
            auto& join_op = op.Cast<duckdb::LogicalComparisonJoin>();
            this->VisitOperatorJoin(join_op, VisitJoinCondition(join_op.conditions));
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_DELIM_JOIN:
        {
            // // push dummy to stack (beause has replaced to cross product)
            auto& join_op = op.Cast<duckdb::LogicalComparisonJoin>();
            this->VisitOperatorJoin(join_op, VisitDelimJoinCondition(join_op));
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_ANY_JOIN:
        {
            auto& join_op = op.Cast<duckdb::LogicalAnyJoin>();

            std::unordered_set<JoinTypeVisitor::Rel> rels{};
            binding::ConditionBindingVisitor visitor(rels);
            visitor.VisitExpression(&join_op.condition);

            this->VisitOperatorJoin(join_op, std::move(JoinTypeVisitor::ConditionRels(rels.begin(), rels.end())));
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
        this->VisitOperatorJoinInternal(op, duckdb::JoinType::OUTER, duckdb::JoinType::OUTER, {});
        break;
    case duckdb::LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
        this->VisitOperatorJoinInternal(op, duckdb::JoinType::INNER, duckdb::JoinType::INNER, {});
        break;
    default:
        duckdb::LogicalOperatorVisitor::VisitOperatorChildren(op);
        break;
    }
}

auto createJoinTypeLookup(duckdb::unique_ptr<duckdb::LogicalOperator>& op) -> std::vector<JoinTypePair> {
    std::vector<JoinTypePair> join_types;

    if (op->type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
        join_types = VisitOperatorProjection(op->Cast<duckdb::LogicalProjection>());
    }

    return std::move(join_types);
}


}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <magic_enum/magic_enum.hpp>

using namespace worker;
using namespace Catch::Matchers;

static auto runCreateJoinTypeLookup(const std::string& sql, std::vector<std::string>&& schemas, const std::vector<JoinTypePair>& expects) -> void {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection conn(db);

    for (auto& schema: schemas) {
        conn.Query(schema);
    }

    auto stmts = conn.ExtractStatements(sql);

    std::vector<JoinTypePair> join_type_result;
    try {
        conn.BeginTransaction();

        auto stmts = conn.ExtractStatements(sql);
        auto bound_statement = bindTypeToStatement(*conn.context, std::move(stmts[0]->Copy()));
        join_type_result = createJoinTypeLookup(bound_statement.plan);

        conn.Commit();
    }
    catch (...) {
        conn.Rollback();
        throw;
    }

    SECTION("Result size") {
        REQUIRE(join_type_result.size() == expects.size());
    }
    SECTION("Result item") {
        for (int i = 0; i < expects.size(); ++i) {
            SECTION(std::format("Result item#{}", i+1)) {
                auto& v = join_type_result[i];
                auto expect = expects[i];

                SECTION("column binding/table_index") {
                    CHECK(v.binding.table_index == expect.binding.table_index);
                }
                SECTION("column binding/column_index") {
                    CHECK(v.binding.column_index == expect.binding.column_index);
                }
                SECTION("Join type") {
                    CHECK_THAT(std::string(magic_enum::enum_name(v.join_type)), Equals(std::string(magic_enum::enum_name(expect.join_type))));
                }
            }
        }
    }
}

TEST_CASE("fromless query") {
    std::string sql(R"#(
        select 123, 'abc'
    )#");

    std::vector<JoinTypePair> expects{};

    runCreateJoinTypeLookup(sql, {}, expects);
}

TEST_CASE("joinless query") {
    std::string schema("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select * from Bar
    )#");

    std::vector<JoinTypePair> expects{
        { .binding = duckdb::ColumnBinding(1, 0), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(1, 1), .join_type = duckdb::JoinType::INNER },
    };

    runCreateJoinTypeLookup(sql, {schema}, expects);
}

TEST_CASE("Inner join") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select * from Foo
        join Bar on Foo.id = Bar.id
    )#");

    std::vector<JoinTypePair> expects{
        { .binding = duckdb::ColumnBinding(2, 0), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(2, 1), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(2, 2), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(2, 3), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(2, 4), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(2, 5), .join_type = duckdb::JoinType::INNER },
    };

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Inner join twice") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select * from Foo
        join Bar b1 on Foo.id = b1.id
        join Bar b2 on Foo.id = b2.id    
    )#");

    std::vector<JoinTypePair> expects{
        { .binding = duckdb::ColumnBinding(3, 0), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 1), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 2), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 3), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 4), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 5), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 6), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 7), .join_type = duckdb::JoinType::INNER },
    };

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Left outer join") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select * from Foo
        left outer join Bar on Foo.id = Bar.id
    )#");

    std::vector<JoinTypePair> expects{
        { .binding = duckdb::ColumnBinding(2, 0), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(2, 1), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(2, 2), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(2, 3), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(2, 4), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(2, 5), .join_type = duckdb::JoinType::OUTER },
    };

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Right outer join") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select * from Foo
        right outer join Bar on Foo.id = Bar.id
    )#");

    std::vector<JoinTypePair> expects{
        { .binding = duckdb::ColumnBinding(2, 0), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(2, 1), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(2, 2), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(2, 3), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(2, 4), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(2, 5), .join_type = duckdb::JoinType::INNER },
    };

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Left outer join twice") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select * from Foo
        left outer join Bar b1 on Foo.id = b1.id
        left outer join Bar b2 on Foo.id = b2.id    
    )#");

    std::vector<JoinTypePair> expects{
        { .binding = duckdb::ColumnBinding(3, 0), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 1), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 2), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 3), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 4), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 5), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 6), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 7), .join_type = duckdb::JoinType::OUTER },
    };

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Inner join + outer join") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select * from Foo
        join Bar b1 on Foo.id = b1.id
        left outer join Bar b2 on Foo.id = b2.id    
    )#");

    std::vector<JoinTypePair> expects{
        { .binding = duckdb::ColumnBinding(3, 0), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 1), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 2), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 3), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 4), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 5), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 6), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 7), .join_type = duckdb::JoinType::OUTER },
    };

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Outer join + inner join#1") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select * from Foo
        left outer join Bar b1 on Foo.id = b1.id
        join Bar b2 on Foo.id = b2.id    
    )#");

    std::vector<JoinTypePair> expects{
        { .binding = duckdb::ColumnBinding(3, 0), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 1), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 2), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 3), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 4), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 5), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 6), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 7), .join_type = duckdb::JoinType::INNER },
    };

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Outer join + inner join#2") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select * from Foo
        left outer join Bar b1 on Foo.id = b1.id
        join Bar b2 on b1.id = b2.id    
    )#");

    std::vector<JoinTypePair> expects{
        { .binding = duckdb::ColumnBinding(3, 0), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 1), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 2), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 3), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 4), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 5), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 6), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 7), .join_type = duckdb::JoinType::OUTER },
    };

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("scalar subquery (single left outer join)") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select 
            Foo.id,
            (
                select Bar.value from Bar
                where bar.id = Foo.id
            ) as v
        from Foo 
    )#");

    std::vector<JoinTypePair> expects{};

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Cross join") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select * from Foo
        cross join Bar
    )#");

    std::vector<JoinTypePair> expects{
        { .binding = duckdb::ColumnBinding(2, 0), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(2, 1), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(2, 2), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(2, 3), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(2, 4), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(2, 5), .join_type = duckdb::JoinType::INNER },
    };

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Cross join + outer join") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select * from Foo
        cross join Bar b1
        left outer join Bar b2 on Foo.id = b2.id
    )#");

    std::vector<JoinTypePair> expects{
        { .binding = duckdb::ColumnBinding(3, 0), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 1), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 2), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 3), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 4), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 5), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(3, 6), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 7), .join_type = duckdb::JoinType::OUTER },
    };

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Full outer join") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select * from Foo
        full outer join Bar on Foo.id = Bar.id
    )#");

    std::vector<JoinTypePair> expects{
        { .binding = duckdb::ColumnBinding(2, 0), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(2, 1), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(2, 2), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(2, 3), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(2, 4), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(2, 5), .join_type = duckdb::JoinType::OUTER },
    };

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Inner join + full outer join#1") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select * from Foo
        join Bar b1 on Foo.id = b1.id
        full outer join Bar b2 on Foo.id = b2.id
    )#");

    std::vector<JoinTypePair> expects{
        { .binding = duckdb::ColumnBinding(3, 0), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 1), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 2), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 3), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 4), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 5), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 6), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 7), .join_type = duckdb::JoinType::OUTER },
    };

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Inner join + full outer join#2") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select * from Foo
        join Bar b1 on Foo.id = b1.id
        full outer join Bar b2 on b1.id = b2.id
    )#");

    std::vector<JoinTypePair> expects{
        { .binding = duckdb::ColumnBinding(3, 0), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 1), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 2), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 3), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 4), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 5), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 6), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 7), .join_type = duckdb::JoinType::OUTER },
    };

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Full outer + inner join join#1") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select * from Foo
        full outer join Bar b2 on Foo.id = b2.id
        join Bar b1 on Foo.id = b1.id
    )#");

    std::vector<JoinTypePair> expects{
        { .binding = duckdb::ColumnBinding(3, 0), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 1), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 2), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 3), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 4), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 5), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 6), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 7), .join_type = duckdb::JoinType::OUTER },
    };

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Full outer + inner join join#2") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select * from Foo
        full outer join Bar b2 on Foo.id = b2.id
        join Bar b1 on b2.id = b1.id
    )#");

    std::vector<JoinTypePair> expects{
        { .binding = duckdb::ColumnBinding(3, 0), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 1), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 2), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 3), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 4), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 5), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 6), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(3, 7), .join_type = duckdb::JoinType::OUTER },
    };

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Positional join") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select * from Foo
        positional join Bar
    )#");

    std::vector<JoinTypePair> expects{
        { .binding = duckdb::ColumnBinding(2, 0), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(2, 1), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(2, 2), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(2, 3), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(2, 4), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(2, 5), .join_type = duckdb::JoinType::OUTER },
    };

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Inner join lateral") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select *
        from Foo 
        join lateral (
          select * from Bar
          where Bar.value = Foo.id
        ) on true
    )#");

    std::vector<JoinTypePair> expects{
        { .binding = duckdb::ColumnBinding(8, 0), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(8, 1), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(8, 2), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(8, 3), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(8, 4), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(8, 5), .join_type = duckdb::JoinType::INNER },
    };

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

TEST_CASE("Outer join lateral") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select *
        from Foo 
        left outer join lateral (
          select * from Bar
          where Bar.value = Foo.id
        ) on true
    )#");

    std::vector<JoinTypePair> expects{
        { .binding = duckdb::ColumnBinding(8, 0), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(8, 1), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(8, 2), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(8, 3), .join_type = duckdb::JoinType::INNER },
        { .binding = duckdb::ColumnBinding(8, 4), .join_type = duckdb::JoinType::OUTER },
        { .binding = duckdb::ColumnBinding(8, 5), .join_type = duckdb::JoinType::OUTER },
    };

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

#endif