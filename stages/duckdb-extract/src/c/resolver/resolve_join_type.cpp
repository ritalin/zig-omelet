#include <ranges>

#include <duckdb.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_delim_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_comparison_join.hpp>
#include <duckdb/planner/operator/logical_any_join.hpp>


#include "duckdb_binder_support.hpp"
#include <iostream>

namespace worker {

class JoinTypeVisitor: public duckdb::LogicalOperatorVisitor {
public:
    using Rel = duckdb::idx_t;
    // using ConditionRels = std::vector<duckdb::idx_t>;
    using ConditionRels = std::map<NullableLookup::Column, NullableLookup::Column>;
public:
    //  JoinTypeVisitor(std::vector<JoinTypePair>& lookup): join_type_lookup(lookup) {
     JoinTypeVisitor(NullableLookup& lookup): join_type_lookup(lookup) {
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
    // std::vector<JoinTypePair>& join_type_lookup;
    NullableLookup& join_type_lookup;
};

static auto EvaluateNullability(duckdb::JoinType rel_join_type, const JoinTypeVisitor::ConditionRels& rel_map, const NullableLookup& internal_lookup, const NullableLookup& lookup) -> bool {
// static auto EvaluateNullability(duckdb::JoinType rel_join_type, JoinTypePair col_join_pair, const JoinTypeVisitor::ConditionRels& conditions, const std::vector<JoinTypePair>& join_types) -> bool {
    if (rel_join_type == duckdb::JoinType::OUTER) return true;
    // if (rel_map.empty()) return false;

    return std::ranges::any_of(rel_map | std::views::values, [&](const auto& to) { return lookup[to]; });

    // for (auto& [from, to]: rel_map) {
    //     if (lookup[to]) return true;
    // }

    // return false;

    // auto join_type_view = 
    //     join_types
    //     | std::views::filter([](const JoinTypePair& p) { return p.nullable; })
    //     | std::views::transform([](const JoinTypePair& p) { return p.binding.table_index; })
    // ;
    // std::unordered_multiset<JoinTypeVisitor::Rel> lookup(join_type_view.begin(), join_type_view.end());

    // auto view = 
    //     conditions
    //     | std::views::filter([&](JoinTypeVisitor::Rel r) { return r != col_join_pair.binding.table_index; })
    //     | std::views::filter([&](JoinTypeVisitor::Rel r) { return lookup.contains(r); })
    // ;

    // return (! view.empty());
}

auto JoinTypeVisitor::VisitOperatorGet(const duckdb::LogicalGet& op) -> void {
    // this->join_type_lookup.reserve(this->join_type_lookup.size() + op.column_ids.size());

    auto view = op.column_ids
        | std::views::transform([&](auto id) { 
            const NullableLookup::Column col{.table_index = op.table_index, .column_index = id};
            return std::pair<const NullableLookup::Column, bool>{std::move(col), false}; 
        })
    ;
    this->join_type_lookup.insert(view.begin(), view.end());


    // for (auto& col: op.column_ids) {
    //     this->join_type_lookup.emplace_back(JoinTypePair {
    //         .binding = duckdb::ColumnBinding(op.table_index, col),
    //         .nullable = false,
    //     });
    // }
}

auto JoinTypeVisitor::VisitOperatorCondition(duckdb::LogicalOperator &op, duckdb::JoinType join_type, const ConditionRels& rels) -> NullableLookup {
// auto JoinTypeVisitor::VisitOperatorCondition(duckdb::LogicalOperator &op, duckdb::JoinType join_type, const ConditionRels& rels) -> std::vector<JoinTypePair> {
    NullableLookup internal_join_types{};
    // std::vector<JoinTypePair> internal_join_types{};
    JoinTypeVisitor visitor(internal_join_types);

    visitor.VisitOperator(op);

    // this->join_type_lookup.reserve(this->join_type_lookup.size() + internal_join_types.size());
    // auto nullable = EvaluateNullability(join_type, rels, internal_join_types, this->join_type_lookup);
    
    // for (auto& [binding, _]: internal_join_types) {
    //     this->join_type_lookup[binding] = nullable;
        // this->join_type_lookup.emplace_back(JoinTypePair {
        //     .binding = std::move(p.binding),
        //     .nullable = EvaluateNullability(join_type, p, rels, this->join_type_lookup),
        // });
    // }

    return std::move(internal_join_types);
}

auto JoinTypeVisitor::VisitOperatorJoinInternal(duckdb::LogicalOperator &op, duckdb::JoinType ty_left, duckdb::JoinType ty_right, ConditionRels&& rels) -> void {
    resolve_join_type: {
        // walk in left
        for (auto& [binding, nullable]: this->VisitOperatorCondition(*op.children[0], ty_left, {})) {
            this->join_type_lookup[binding] = nullable || (ty_left == duckdb::JoinType::OUTER);
        }

        // walk in right
        auto internal_join_types = this->VisitOperatorCondition(*op.children[1], ty_right, rels);
        auto nullable = EvaluateNullability(ty_right, rels, internal_join_types, this->join_type_lookup);

        for (auto& binding: internal_join_types | std::views::keys) {
            this->join_type_lookup[binding] = nullable;
        }
    }
}

namespace binding {
    class ConditionBindingVisitor: public duckdb::LogicalOperatorVisitor {
    public:
        ConditionBindingVisitor(JoinTypeVisitor::ConditionRels& rels): condition_rels(rels) {}
    protected:
        auto VisitReplace(duckdb::BoundColumnRefExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
            NullableLookup::Column col{
                .table_index = expr.binding.table_index, 
                .column_index = expr.binding.column_index, 
            };
            this->condition_rels[col] = col;

            return nullptr;
        }
    private:
        JoinTypeVisitor::ConditionRels& condition_rels;
    };

    class DelimGetBindingVisitor: public duckdb::LogicalOperatorVisitor {
    public:
        DelimGetBindingVisitor(JoinTypeVisitor::ConditionRels& rels, std::vector<NullableLookup::Column>&& map_to)
            : condition_rels(rels), map_to(map_to)
        {
        }
    public:
        auto VisitOperator(duckdb::LogicalOperator &op) -> void {
            if (op.type == duckdb::LogicalOperatorType::LOGICAL_DELIM_GET) {
                auto& op_get = op.Cast<duckdb::LogicalDelimGet>();
                for (duckdb::idx_t i = 0; i < op_get.chunk_types.size(); ++i) {
                    NullableLookup::Column from{
                        .table_index = op_get.table_index, 
                        .column_index = i, 
                    };
                    this->condition_rels[from] = this->map_to[i];
                    
                }
            }
            else {
                duckdb::LogicalOperatorVisitor::VisitOperator(op);
            }
        }
    private:
        JoinTypeVisitor::ConditionRels& condition_rels;
        std::vector<NullableLookup::Column> map_to;
    };
}

static auto VisitJoinCondition(duckdb::vector<duckdb::JoinCondition>& conditions) -> JoinTypeVisitor::ConditionRels {
    JoinTypeVisitor::ConditionRels rel_map{};
    condition: {
        binding::ConditionBindingVisitor visitor(rel_map);

        for (auto& c: conditions) {
            visitor.VisitExpression(&c.left);
            visitor.VisitExpression(&c.right);
        }
    }

    return std::move(rel_map);
}

static auto VisitDelimJoinCondition(duckdb::LogicalComparisonJoin& op, std::vector<NullableLookup::Column>&& map_to) -> JoinTypeVisitor::ConditionRels {
    JoinTypeVisitor::ConditionRels rel_map{};
    delim_get: {
        binding::DelimGetBindingVisitor visitor(rel_map, std::move(map_to));

        for (auto& c: op.children) {
            visitor.VisitOperator(*c);
        }
    }
    condition: {
        JoinTypeVisitor::ConditionRels rel_map2{};
        binding::ConditionBindingVisitor visitor(rel_map2);

        for (auto& c: op.conditions) {
            visitor.VisitExpression(&c.left);
            visitor.VisitExpression(&c.right);
        }

        rel_map.insert(rel_map2.begin(), rel_map2.end());
    }

    return std::move(rel_map);
}

static auto ToOuterAll(NullableLookup& lookup) -> void {
    for (auto& nullable: lookup | std::views::values) {
        nullable = true;
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

static auto isNullable(const std::vector<JoinTypePair>& join_types, duckdb::ColumnBinding binding) -> bool {
    auto iter = std::ranges::find_if(join_types, [&](const JoinTypePair& p) { 
        return p.binding == binding; 
    });

    // if (iter == join_types.end()) {
    //     std::cout << std::format("binding: ({},{})", binding.table_index, binding.column_index) << std::endl;
    //     std::cout << std::format("join_types/size: {}", join_types.size()) << std::endl;
    //     std::cout << std::format("join_types/bindings: {}", debugDumpJoinTypeBindings(join_types)) << std::endl;
    // }

    return iter->nullable;
}

static auto VisitOperatorProjection(duckdb::LogicalProjection& op) -> NullableLookup {
// static auto VisitOperatorProjection(duckdb::LogicalProjection& op) -> std::vector<JoinTypePair> {
    NullableLookup internal_join_types{};
    JoinTypeVisitor visitor(internal_join_types);

    for (auto& child: op.children) {
        visitor.VisitOperator(*child);
    }

    NullableLookup results;
    // results.reserve(op.expressions.size());

    if (internal_join_types.empty()) {
        return results;
    }

    // for (duckdb::idx_t i = 0; auto& expr: op.expressions) {
    //     duckdb::ColumnBinding binding(op.table_index, i);

    //     if (expr->expression_class == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    //         auto& expr_column = expr->Cast<duckdb::BoundColumnRefExpression>();
    //         results.emplace_back(JoinTypePair {.binding = binding, .nullable = isNullable(internal_join_types, expr_column.binding)});
    //     }
    //     else {
    //         results.emplace_back(JoinTypePair {.binding = binding, .nullable = false});
    //     }
    //     ++i;
    // }

    for (duckdb::idx_t i = 0; auto& expr: op.expressions) {
        duckdb::ColumnBinding binding(op.table_index, i);

        if (expr->expression_class == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
            auto& expr_column = expr->Cast<duckdb::BoundColumnRefExpression>();

            results[binding] = internal_join_types[expr_column.binding];
        }
        else {
            results[binding] = false;
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
            // this->join_type_lookup.insert(this->join_type_lookup.end(), lookup.begin(), lookup.end());
            this->join_type_lookup.insert(lookup.begin(), lookup.end());
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
            auto& join_op = op.Cast<duckdb::LogicalComparisonJoin>();

            auto view = join_op.duplicate_eliminated_columns
                | std::views::filter([](auto& expr) { return expr->type == duckdb::ExpressionType::BOUND_COLUMN_REF; })
                | std::views::transform([](duckdb::unique_ptr<duckdb::Expression>& expr) { 
                    auto& c = expr->Cast<duckdb::BoundColumnRefExpression>();
                    return NullableLookup::Column{ .table_index = c.binding.table_index, .column_index = c.binding.column_index };
                })
            ;

            this->VisitOperatorJoin(join_op, VisitDelimJoinCondition(join_op, std::vector(view.begin(), view.end())));
        }
        break;
    case duckdb::LogicalOperatorType::LOGICAL_ANY_JOIN:
        {
            auto& join_op = op.Cast<duckdb::LogicalAnyJoin>();

            ConditionRels rel_map{};
            binding::ConditionBindingVisitor visitor(rel_map);
            visitor.VisitExpression(&join_op.condition);

            this->VisitOperatorJoin(join_op, std::move(rel_map));
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

auto createJoinTypeLookup(duckdb::unique_ptr<duckdb::LogicalOperator>& op) -> NullableLookup {
    NullableLookup lookup;

    if (op->type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
        lookup = VisitOperatorProjection(op->Cast<duckdb::LogicalProjection>());
    }

    return std::move(lookup);
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

        NullableLookup join_type_result;
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

    Result_size: {
        UNSCOPED_INFO("Result size");
        REQUIRE(join_type_result.size() == expects.size());
    }
    Result_items: {    
        for (int i = 0; i < expects.size(); ++i) {
            auto expect = expects[i];

            INFO(std::format("Result item#{}", i+1)); 

            UNSCOPED_INFO("has column binding");
            CHECK(join_type_result.contains(expect.binding.table_index, expect.binding.column_index));
            
            UNSCOPED_INFO("column nullability");
            CHECK(join_type_result[expect.binding] == expect.nullable);
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
        { .binding = duckdb::ColumnBinding(1, 0), .nullable = false },
        { .binding = duckdb::ColumnBinding(1, 1), .nullable = false },
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
        { .binding = duckdb::ColumnBinding(2, 0), .nullable = false },
        { .binding = duckdb::ColumnBinding(2, 1), .nullable = false },
        { .binding = duckdb::ColumnBinding(2, 2), .nullable = false },
        { .binding = duckdb::ColumnBinding(2, 3), .nullable = false },
        { .binding = duckdb::ColumnBinding(2, 4), .nullable = false },
        { .binding = duckdb::ColumnBinding(2, 5), .nullable = false },
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
        { .binding = duckdb::ColumnBinding(3, 0), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 1), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 2), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 3), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 4), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 5), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 6), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 7), .nullable = false },
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
        { .binding = duckdb::ColumnBinding(2, 0), .nullable = false },
        { .binding = duckdb::ColumnBinding(2, 1), .nullable = false },
        { .binding = duckdb::ColumnBinding(2, 2), .nullable = false },
        { .binding = duckdb::ColumnBinding(2, 3), .nullable = false },
        { .binding = duckdb::ColumnBinding(2, 4), .nullable = true },
        { .binding = duckdb::ColumnBinding(2, 5), .nullable = true },
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
        { .binding = duckdb::ColumnBinding(2, 0), .nullable = true },
        { .binding = duckdb::ColumnBinding(2, 1), .nullable = true },
        { .binding = duckdb::ColumnBinding(2, 2), .nullable = true },
        { .binding = duckdb::ColumnBinding(2, 3), .nullable = true },
        { .binding = duckdb::ColumnBinding(2, 4), .nullable = false },
        { .binding = duckdb::ColumnBinding(2, 5), .nullable = false },
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
        { .binding = duckdb::ColumnBinding(3, 0), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 1), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 2), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 3), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 4), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 5), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 6), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 7), .nullable = true },
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
        { .binding = duckdb::ColumnBinding(3, 0), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 1), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 2), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 3), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 4), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 5), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 6), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 7), .nullable = true },
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
        { .binding = duckdb::ColumnBinding(3, 0), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 1), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 2), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 3), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 4), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 5), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 6), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 7), .nullable = false },
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
        { .binding = duckdb::ColumnBinding(3, 0), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 1), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 2), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 3), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 4), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 5), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 6), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 7), .nullable = true },
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
        { .binding = duckdb::ColumnBinding(2, 0), .nullable = false },
        { .binding = duckdb::ColumnBinding(2, 1), .nullable = false },
        { .binding = duckdb::ColumnBinding(2, 2), .nullable = false },
        { .binding = duckdb::ColumnBinding(2, 3), .nullable = false },
        { .binding = duckdb::ColumnBinding(2, 4), .nullable = false },
        { .binding = duckdb::ColumnBinding(2, 5), .nullable = false },
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
        { .binding = duckdb::ColumnBinding(3, 0), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 1), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 2), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 3), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 4), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 5), .nullable = false },
        { .binding = duckdb::ColumnBinding(3, 6), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 7), .nullable = true },
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
        { .binding = duckdb::ColumnBinding(2, 0), .nullable = true },
        { .binding = duckdb::ColumnBinding(2, 1), .nullable = true },
        { .binding = duckdb::ColumnBinding(2, 2), .nullable = true },
        { .binding = duckdb::ColumnBinding(2, 3), .nullable = true },
        { .binding = duckdb::ColumnBinding(2, 4), .nullable = true },
        { .binding = duckdb::ColumnBinding(2, 5), .nullable = true },
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
        { .binding = duckdb::ColumnBinding(3, 0), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 1), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 2), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 3), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 4), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 5), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 6), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 7), .nullable = true },
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
        { .binding = duckdb::ColumnBinding(3, 0), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 1), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 2), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 3), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 4), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 5), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 6), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 7), .nullable = true },
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
        { .binding = duckdb::ColumnBinding(3, 0), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 1), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 2), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 3), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 4), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 5), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 6), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 7), .nullable = true },
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
        { .binding = duckdb::ColumnBinding(3, 0), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 1), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 2), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 3), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 4), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 5), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 6), .nullable = true },
        { .binding = duckdb::ColumnBinding(3, 7), .nullable = true },
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
        { .binding = duckdb::ColumnBinding(2, 0), .nullable = true },
        { .binding = duckdb::ColumnBinding(2, 1), .nullable = true },
        { .binding = duckdb::ColumnBinding(2, 2), .nullable = true },
        { .binding = duckdb::ColumnBinding(2, 3), .nullable = true },
        { .binding = duckdb::ColumnBinding(2, 4), .nullable = true },
        { .binding = duckdb::ColumnBinding(2, 5), .nullable = true },
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
        { .binding = duckdb::ColumnBinding(8, 0), .nullable = false },
        { .binding = duckdb::ColumnBinding(8, 1), .nullable = false },
        { .binding = duckdb::ColumnBinding(8, 2), .nullable = false },
        { .binding = duckdb::ColumnBinding(8, 3), .nullable = false },
        { .binding = duckdb::ColumnBinding(8, 4), .nullable = false },
        { .binding = duckdb::ColumnBinding(8, 5), .nullable = false },
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
        { .binding = duckdb::ColumnBinding(8, 0), .nullable = false },
        { .binding = duckdb::ColumnBinding(8, 1), .nullable = false },
        { .binding = duckdb::ColumnBinding(8, 2), .nullable = false },
        { .binding = duckdb::ColumnBinding(8, 3), .nullable = false },
        { .binding = duckdb::ColumnBinding(8, 4), .nullable = true },
        { .binding = duckdb::ColumnBinding(8, 5), .nullable = true },
    };

    runCreateJoinTypeLookup(sql, {schema_1, schema_2}, expects);
}

#endif