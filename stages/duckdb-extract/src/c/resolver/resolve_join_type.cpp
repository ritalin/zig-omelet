#include <ranges>

#include <duckdb.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_comparison_join.hpp>
#include <duckdb/planner/operator/logical_any_join.hpp>


#include "duckdb_binder_support.hpp"

namespace worker {

class JoinTypeVisitor: public duckdb::LogicalOperatorVisitor {
public:
    using Rel = duckdb::idx_t;
    using ConditionRels = std::vector<duckdb::idx_t>;
public:
     JoinTypeVisitor(std::unordered_map<Rel, duckdb::JoinType>& lookup): join_type_lookup(lookup), stack() {
        stack.push(std::make_pair<duckdb::JoinType, ConditionRels>(duckdb::JoinType::INNER, {})); // dummy
     }
public:
    auto VisitOperator(duckdb::LogicalOperator &op) -> void;
private:
    auto VisitOperatorGet(const duckdb::LogicalGet& op) -> void;
    auto VisitOperatorJoin(duckdb::LogicalJoin& op, ConditionRels&& rels) -> void;
    auto VisitOperatorJoinInternal(duckdb::LogicalOperator &op, duckdb::JoinType ty_left, duckdb::JoinType ty_right, ConditionRels&& rels) -> void;
private:
    std::stack<std::pair<duckdb::JoinType, ConditionRels>> stack;
    std::unordered_map<Rel, duckdb::JoinType>& join_type_lookup;
};

static auto EvaluateNullability(JoinTypeVisitor::Rel rel, const JoinTypeVisitor::ConditionRels& conditions, const std::unordered_map<JoinTypeVisitor::Rel, duckdb::JoinType>& lookup) -> duckdb::JoinType {
    auto view = 
        conditions
        | std::views::filter([&](JoinTypeVisitor::Rel r) { return r != rel; })
        | std::views::transform([&](JoinTypeVisitor::Rel r) { return lookup.at(r); } )
    ;

    if (std::ranges::any_of(view, [](duckdb::JoinType ty) { return ty == duckdb::JoinType::OUTER; })) {
        return duckdb::JoinType::OUTER;
    }
    else {
        return duckdb::JoinType::INNER;
    }
}

auto JoinTypeVisitor::VisitOperatorGet(const duckdb::LogicalGet& op) -> void {
    auto [ty, rels] = this->stack.top(); this->stack.pop();
    
    if (ty == duckdb::JoinType::OUTER) {
        this->join_type_lookup[op.table_index] = ty;
    }
    else if (ty == duckdb::JoinType::INNER) {
        this->join_type_lookup[op.table_index] = EvaluateNullability(op.table_index, rels, this->join_type_lookup);
    }
}

auto JoinTypeVisitor::VisitOperatorJoinInternal(duckdb::LogicalOperator &op, duckdb::JoinType ty_left, duckdb::JoinType ty_right, ConditionRels&& rels) -> void {
    this->stack.push(std::make_pair<duckdb::JoinType, ConditionRels>(std::move(ty_right), std::move(rels)));
    this->stack.push(std::make_pair<duckdb::JoinType, ConditionRels>(std::move(ty_left), {}));

    // walk in left
    this->VisitOperator(*op.children[0]);
    // walk in right
    this->VisitOperator(*op.children[1]);
    // drop
    this->stack.pop();
}

namespace binding {
    class ConditionBindingVisitor: public duckdb::LogicalOperatorVisitor {
    public:
        ConditionBindingVisitor(JoinTypeVisitor::ConditionRels& rels): condition_rels(rels) {}
    protected:
        auto VisitReplace(duckdb::BoundColumnRefExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
            this->condition_rels.push_back(expr.binding.table_index);
            return nullptr;
        }
    private:
        JoinTypeVisitor::ConditionRels& condition_rels;
    };
}

static auto VisitJoinCondition(duckdb::vector<duckdb::JoinCondition>& conditions) -> JoinTypeVisitor::ConditionRels {
    JoinTypeVisitor::ConditionRels rels{};
    binding::ConditionBindingVisitor visitor(rels);

    for (auto& c: conditions) {
        visitor.VisitExpression(&c.left);
        visitor.VisitExpression(&c.right);
    }

    return std::move(rels);
}

static auto ToOuterAll(std::unordered_map<JoinTypeVisitor::Rel, duckdb::JoinType>& lookup) -> void {
    for (auto& k: lookup | std::views::keys) {
        lookup[k] = duckdb::JoinType::OUTER;
    }
}

auto JoinTypeVisitor::VisitOperatorJoin(duckdb::LogicalJoin& op, ConditionRels&& rels) -> void {
    if (op.join_type == duckdb::JoinType::INNER) {
        this->VisitOperatorJoinInternal(op, duckdb::JoinType::INNER, duckdb::JoinType::INNER, std::move(rels));
    }
    else if (op.join_type == duckdb::JoinType::LEFT) {
        this->VisitOperatorJoinInternal(op, duckdb::JoinType::INNER, duckdb::JoinType::OUTER, {});
    }
    else if (op.join_type == duckdb::JoinType::RIGHT) {
        this->VisitOperatorJoinInternal(op, duckdb::JoinType::OUTER, duckdb::JoinType::INNER, std::move(rels));
    }
    else if (op.join_type == duckdb::JoinType::OUTER) {
        this->VisitOperatorJoinInternal(op, duckdb::JoinType::OUTER, duckdb::JoinType::OUTER, {});
        ToOuterAll(this->join_type_lookup);
    }
}

auto JoinTypeVisitor::VisitOperator(duckdb::LogicalOperator &op) -> void {
    switch (op.type) {
    case duckdb::LogicalOperatorType::LOGICAL_GET:
        this->VisitOperatorGet(op.Cast<duckdb::LogicalGet>());
        return;
    case duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
    case duckdb::LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
        {
            auto& join_op = op.Cast<duckdb::LogicalComparisonJoin>();
            this->VisitOperatorJoin(join_op, VisitJoinCondition(join_op.conditions));
        }
        return;
    case duckdb::LogicalOperatorType::LOGICAL_ANY_JOIN:
        {
            auto& join_op = op.Cast<duckdb::LogicalAnyJoin>();

            JoinTypeVisitor::ConditionRels rels{};
            binding::ConditionBindingVisitor visitor(rels);
            visitor.VisitExpression(&join_op.condition);

            this->VisitOperatorJoin(join_op, std::move(rels));
        }
        return;
    case duckdb::LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
        this->VisitOperatorJoinInternal(op, duckdb::JoinType::OUTER, duckdb::JoinType::OUTER, {});
        return;
    case duckdb::LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
        this->VisitOperatorJoinInternal(op, duckdb::JoinType::INNER, duckdb::JoinType::INNER, {});
        return;
    default:
        break;
    }
    
    duckdb::LogicalOperatorVisitor::VisitOperatorChildren(op);
}

auto createJoinTypeLookup(duckdb::unique_ptr<duckdb::LogicalOperator>& op) -> std::vector<JoinTypePair> {
    std::unordered_map<JoinTypeVisitor::Rel, duckdb::JoinType> lookup{};

    JoinTypeVisitor visitor(lookup);
    visitor.VisitOperator(*op);

    auto view = lookup | std::views::transform([](auto x) { 
        return JoinTypePair{.table_index = x.first, .join_type = x.second}; 
    });

    return std::move(std::vector<JoinTypePair>(view.begin(), view.end()));
}


}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

using namespace worker;
using namespace Catch::Matchers;

static auto runCreateJoinTypeLookup(const std::string& sql, std::vector<std::string>&& schemas, std::vector<JoinTypePair>& expects) -> void {
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
        auto view = expects | std::views::transform([](JoinTypePair x) { 
            return std::make_pair<duckdb::idx_t, duckdb::JoinType>(std::move(x.table_index), std::move(x.join_type)); 
        });
        auto expected_map = std::unordered_map<duckdb::idx_t, duckdb::JoinType>(view.begin(), view.end());
        
        for (int i = 1; auto& v: join_type_result) {
            SECTION(std::format("Result item#{}", i)) {
                SECTION("table index") {
                    CHECK(expected_map.contains(v.table_index));
                }
                SECTION("Join type") {
                    CHECK(v.join_type == expected_map.at(v.table_index));
                }
            }
            ++i;
        }
    }
}

TEST_CASE("Inner join") {
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");
    std::string sql(R"#(
        select * from Foo
        join Bar on Foo.id = Bar.id
    )#");

    std::vector<JoinTypePair> expects{
        {.table_index = 0, .join_type = duckdb::JoinType::INNER},
        {.table_index = 1, .join_type = duckdb::JoinType::INNER},
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
        {.table_index = 0, .join_type = duckdb::JoinType::INNER},
        {.table_index = 1, .join_type = duckdb::JoinType::INNER},
        {.table_index = 2, .join_type = duckdb::JoinType::INNER},
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
        {.table_index = 0, .join_type = duckdb::JoinType::INNER},
        {.table_index = 1, .join_type = duckdb::JoinType::OUTER},
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
        {.table_index = 0, .join_type = duckdb::JoinType::OUTER},
        {.table_index = 1, .join_type = duckdb::JoinType::INNER},
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
        {.table_index = 0, .join_type = duckdb::JoinType::INNER},
        {.table_index = 1, .join_type = duckdb::JoinType::OUTER},
        {.table_index = 2, .join_type = duckdb::JoinType::OUTER},
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
        {.table_index = 0, .join_type = duckdb::JoinType::INNER},
        {.table_index = 1, .join_type = duckdb::JoinType::INNER},
        {.table_index = 2, .join_type = duckdb::JoinType::OUTER},
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
        {.table_index = 0, .join_type = duckdb::JoinType::INNER},
        {.table_index = 1, .join_type = duckdb::JoinType::OUTER},
        {.table_index = 2, .join_type = duckdb::JoinType::INNER},
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
        {.table_index = 0, .join_type = duckdb::JoinType::INNER},
        {.table_index = 1, .join_type = duckdb::JoinType::OUTER},
        {.table_index = 2, .join_type = duckdb::JoinType::OUTER},
    };

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
        {.table_index = 0, .join_type = duckdb::JoinType::INNER},
        {.table_index = 1, .join_type = duckdb::JoinType::INNER},
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
        {.table_index = 0, .join_type = duckdb::JoinType::INNER},
        {.table_index = 1, .join_type = duckdb::JoinType::INNER},
        {.table_index = 2, .join_type = duckdb::JoinType::OUTER},
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
        {.table_index = 0, .join_type = duckdb::JoinType::OUTER},
        {.table_index = 1, .join_type = duckdb::JoinType::OUTER},
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
        {.table_index = 0, .join_type = duckdb::JoinType::OUTER},
        {.table_index = 1, .join_type = duckdb::JoinType::OUTER},
        {.table_index = 2, .join_type = duckdb::JoinType::OUTER},
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
        {.table_index = 0, .join_type = duckdb::JoinType::OUTER},
        {.table_index = 1, .join_type = duckdb::JoinType::OUTER},
        {.table_index = 2, .join_type = duckdb::JoinType::OUTER},
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
        {.table_index = 0, .join_type = duckdb::JoinType::OUTER},
        {.table_index = 1, .join_type = duckdb::JoinType::OUTER},
        {.table_index = 2, .join_type = duckdb::JoinType::OUTER},
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
        {.table_index = 0, .join_type = duckdb::JoinType::OUTER},
        {.table_index = 1, .join_type = duckdb::JoinType::OUTER},
        {.table_index = 2, .join_type = duckdb::JoinType::OUTER},
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
        {.table_index = 0, .join_type = duckdb::JoinType::OUTER},
        {.table_index = 1, .join_type = duckdb::JoinType::OUTER},
    };

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

#endif