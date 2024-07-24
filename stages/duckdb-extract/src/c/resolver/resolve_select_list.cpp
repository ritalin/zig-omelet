#include <ranges>

#include <duckdb.hpp>
#include <duckdb/planner/logical_operator_visitor.hpp>
#include <duckdb/planner/expression/list.hpp>
#include <duckdb/planner/tableref/list.hpp>
#include <duckdb/parser/constraints/list.hpp>
#include <duckdb/catalog/catalog_entry/list.hpp>
#include <duckdb/planner/operator/logical_get.hpp>

#include "duckdb_binder_support.hpp"

#include <iostream>
#include <magic_enum/magic_enum.hpp>

namespace worker {

class SelectListVisitor: public duckdb::LogicalOperatorVisitor {
private: 
    class DummyExpression: public duckdb::Expression {
    public:
        DummyExpression(): duckdb::Expression(duckdb::ExpressionType::INVALID, duckdb::ExpressionClass::INVALID, duckdb::LogicalType::SQLNULL) {}
        auto ToString() const -> std::string { return ""; }
        auto Copy() -> duckdb::unique_ptr<Expression> { return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression()); }
    };
public:
    SelectListVisitor(duckdb::unique_ptr<duckdb::BoundTableRef>&& table_ref, std::vector<ColumnBindingPair>&& bindings, std::vector<JoinTypePair>&& join_types)
        : table_ref(std::move(table_ref)), column_bindings(bindings), join_types(join_types)
    {
    }
public:
    auto Visit(duckdb::unique_ptr<duckdb::LogicalOperator>& op) -> std::vector<ColumnEntry>;
    auto VisitNullability(duckdb::unique_ptr<duckdb::Expression>& expr) -> bool;
public:
    auto VisitExpressionChildren(duckdb::Expression& expr) -> void;
protected:
	auto VisitReplace(duckdb::BoundConstantExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
	auto VisitReplace(duckdb::BoundComparisonExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
	auto VisitReplace(duckdb::BoundOperatorExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
    auto VisitReplace(duckdb::BoundFunctionExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
	auto VisitReplace(duckdb::BoundColumnRefExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;
	auto VisitReplace(duckdb::BoundParameterExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression>;

private:
    auto VisitNullabilityInternal(duckdb::unique_ptr<duckdb::Expression>& expr) -> bool;
    template <typename Expr>
    auto VisitNullabilityChildren(Expr& expr, bool terminate_value) -> bool;
private:
    duckdb::unique_ptr<duckdb::BoundTableRef> table_ref;
    std::stack<bool> nullable_result;
    std::vector<ColumnBindingPair> column_bindings;
    std::vector<JoinTypePair> join_types;
};

auto SelectListVisitor::VisitExpressionChildren(duckdb::Expression& expr) -> void {
    if (dynamic_cast<DummyExpression*>(&expr) != nullptr) {
        return;
    }
    else {
        duckdb::LogicalOperatorVisitor::VisitExpressionChildren(expr);
    }
}

auto SelectListVisitor::VisitReplace(duckdb::BoundConstantExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    this->nullable_result.push(expr.value.IsNull());

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto SelectListVisitor::VisitReplace(duckdb::BoundParameterExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    this->nullable_result.push(true);

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto SelectListVisitor::VisitReplace(duckdb::BoundComparisonExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    switch (expr.type) {
    case duckdb::ExpressionType::COMPARE_DISTINCT_FROM: 
        // is [not] false/true
        this->nullable_result.push(false);
        break;
    default: 
        this->nullable_result.push(this->VisitNullabilityInternal(expr.left) | this->VisitNullabilityInternal(expr.right));
        break;
    }

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto SelectListVisitor::VisitReplace(duckdb::BoundOperatorExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    auto nullable = false;

    switch (expr.type) {
    case duckdb::ExpressionType::OPERATOR_IS_NULL:
    case duckdb::ExpressionType::OPERATOR_IS_NOT_NULL:
        // always NOT null
        break;
    case duckdb::ExpressionType::OPERATOR_COALESCE:
    case duckdb::ExpressionType::OPERATOR_NULLIF:
        nullable = this->VisitNullabilityChildren(expr, false);
        break;
    default: 
        nullable = this->VisitNullabilityChildren(expr, true);
        break;
    }
    
    this->nullable_result.push(nullable);

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto SelectListVisitor::VisitReplace(duckdb::BoundFunctionExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    if (expr.is_operator) {
        this->nullable_result.push(this->VisitNullabilityChildren(expr, true));
    }
    else {
        this->nullable_result.push(true);
    }

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

static auto hasNotNullConstraintInternal(duckdb::idx_t column_index, const duckdb::TableCatalogEntry& catalog) -> bool {    
    auto null_constraints = 
        catalog.GetConstraints()
        | std::views::filter([](const duckdb::unique_ptr<duckdb::Constraint>& c) { return c->type == duckdb::ConstraintType::NOT_NULL; })
        | std::views::transform([](const duckdb::unique_ptr<duckdb::Constraint>& c) { return c->Cast<duckdb::NotNullConstraint>(); })
    ;
    
    return std::ranges::any_of(
        null_constraints,
        [&](auto x) { return x.index.index == column_index; }
    );
}

static auto isOuterJoinType(const std::vector<JoinTypePair>& join_types, const duckdb::ColumnBinding& binding) -> bool {
    return std::ranges::any_of(join_types, [&](JoinTypePair x) {
        return (x.table_index == binding.table_index) && (x.join_type == duckdb::JoinType::OUTER);
    });
}

static auto resolveColumnBinding(const std::vector<JoinTypePair>& join_types, const std::vector<ColumnBindingPair>& bindings, const duckdb::ColumnBinding& from) -> std::optional<duckdb::ColumnBinding> {
    if (isOuterJoinType(join_types, from)) {
        return std::nullopt;
    }

    auto iter = std::ranges::find_if(bindings, [&](const duckdb::ColumnBinding& binding) { return binding == from; }, &ColumnBindingPair::from);

    return iter != bindings.end() ? std::make_optional(iter->to) : std::nullopt;
}

static auto hasNotNullConstraint(const duckdb::ColumnBinding& binding, const duckdb::unique_ptr<duckdb::BoundTableRef>& table_ref) -> std::optional<bool> {
    // std::cout << std::format("Table/kind: {}", magic_enum::enum_name(table_ref->type)) << std::endl;
    switch (table_ref->type) {
    case duckdb::TableReferenceType::BASE_TABLE:
        {
            auto& base_table_ref = table_ref->Cast<duckdb::BoundBaseTableRef>();
            if (base_table_ref.get->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
                throw duckdb::Exception(duckdb::ExceptionType::UNKNOWN_TYPE, std::format("Unexpected op type (“{}“) of duckdb::BaseTableRef", magic_enum::enum_name(base_table_ref.get->type)));
            }

            auto& op_get = base_table_ref.get->Cast<duckdb::LogicalGet>();
            // std::cout << std::format("Table/index: {}", op_get.table_index) << std::endl;
            if (op_get.table_index != binding.table_index) {
                return std::nullopt;
            }

            return hasNotNullConstraintInternal(binding.column_index, base_table_ref.table);
        }
    case duckdb::TableReferenceType::JOIN: 
        {
            auto& joined_table_ref = table_ref->Cast<duckdb::BoundJoinRef>();

            auto left_result = hasNotNullConstraint(binding, joined_table_ref.left);
            if (left_result) return left_result;

            return hasNotNullConstraint(binding, joined_table_ref.right);
        }
        break;
    default:
        return std::nullopt;
    }
}

auto SelectListVisitor::VisitReplace(duckdb::BoundColumnRefExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
    auto not_null = false;
    auto binding = resolveColumnBinding(this->join_types, this->column_bindings, expr.binding);
    if (binding) {
        auto not_null_result = hasNotNullConstraint(binding.value(), this->table_ref);
        not_null = (bool)not_null_result ? not_null_result.value() : false;
    }
    this->nullable_result.push(! not_null);

    return duckdb::unique_ptr<duckdb::Expression>(new DummyExpression());
}

auto SelectListVisitor::VisitNullabilityInternal(duckdb::unique_ptr<duckdb::Expression>& expr) -> bool {
    size_t depth = this->nullable_result.size();

    this->VisitExpression(&expr);

    auto result = false;
    if (this->nullable_result.size() > depth) {
        result = this->nullable_result.top();
        this->nullable_result.pop();
    }

    return result;
}

template <typename Expr>
auto SelectListVisitor::VisitNullabilityChildren(Expr& expr, bool terminate_value) -> bool {
    for (auto& child: expr.children) {
        if (this->VisitNullabilityInternal(child) == terminate_value) {
            return terminate_value;
        }
    }

    return (! terminate_value);
}

auto SelectListVisitor::VisitNullability(duckdb::unique_ptr<duckdb::Expression>& expr) -> bool {
    this->nullable_result = {};
    return this->VisitNullabilityInternal(expr);
}

namespace column_name {
    class ColumnNameVisitor: public duckdb::LogicalOperatorVisitor {
    public:
        ColumnNameVisitor(std::string& name): column_name(name) {}
    public:
        static auto Visit(duckdb::unique_ptr<duckdb::Expression>& expr) -> std::string {
            if (expr->alias != "") return expr->alias;

            std::string result;
            ColumnNameVisitor visitor(result);
            visitor.VisitExpression(&expr);

            return result;
        }
    protected:
        auto VisitReplace(duckdb::BoundConstantExpression &expr, duckdb::unique_ptr<duckdb::Expression> *expr_ptr) -> duckdb::unique_ptr<duckdb::Expression> {
            this->column_name = expr.value.ToSQLString();
            return nullptr;
        }
    private:
        std::string& column_name;
    };
}

auto SelectListVisitor::Visit(duckdb::unique_ptr<duckdb::LogicalOperator>& op) -> std::vector<ColumnEntry> {
    std::vector<ColumnEntry> result;
    result.reserve(op->expressions.size());

    std::unordered_multiset<std::string> name_dupe{};

    for (size_t i = 0; auto& expr: op->expressions) {
        auto field_name = column_name::ColumnNameVisitor::Visit(expr);

        auto dupe_count = name_dupe.count(field_name);
        name_dupe.insert(field_name);

        auto entry = ColumnEntry{
            .field_name = dupe_count == 0 ? field_name : std::format("{}_{}", field_name, dupe_count),
            .field_type = expr->return_type.ToString(),
            .nullable = this->VisitNullability(expr),
        };
        // std::cout << std::format("Entry/name: {}, type: {}, nullable: {}", entry.field_name, entry.field_type, entry.nullable) << std::endl << std::endl;
        result.emplace_back(std::move(entry));
    }

    return std::move(result);
}

auto resolveColumnType(duckdb::unique_ptr<duckdb::LogicalOperator>& op, duckdb::unique_ptr<duckdb::BoundTableRef>&& table_ref, StatementType stmt_type) -> std::vector<ColumnEntry> {
    if (stmt_type != StatementType::Select) return {};

    auto lookup = std::move(createColumnBindingLookup(op));
    auto join_types = std::move(createJoinTypeLookup(op));

    SelectListVisitor visitor(
        std::forward<duckdb::unique_ptr<duckdb::BoundTableRef>>(table_ref), 
        std::forward<std::vector<ColumnBindingPair>>(lookup),
        std::forward<std::vector<JoinTypePair>>(join_types)
    );
    return visitor.Visit(op);
}

}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <utility>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

using namespace worker;
using namespace Catch::Matchers;

using LogicalOperatorRef = duckdb::unique_ptr<duckdb::LogicalOperator>;
using BoundTableRef = duckdb::unique_ptr<duckdb::BoundTableRef>;

static auto runBindStatement(const std::string sql, const std::vector<std::string>& schemas, const std::vector<ColumnEntry>& expected) -> void {
    auto db = duckdb::DuckDB(nullptr);
    auto conn = duckdb::Connection(db);

    prepare_schema: {
        for (auto& schema: schemas) {
            conn.Query(schema);
        }
    }

    std::vector<ColumnEntry> column_result;
    try {
        conn.BeginTransaction();

        auto stmts = conn.ExtractStatements(sql);
        auto stmt_type = evalStatementType(stmts[0]);

        auto bound_statement = bindTypeToStatement(*conn.context, std::move(stmts[0]->Copy()));
        auto bound_table_ref = bindTypeToTableRef(*conn.context, std::move(stmts[0]->Copy()), stmt_type);

        column_result = resolveColumnType(bound_statement.plan, std::forward<BoundTableRef>(bound_table_ref), stmt_type);
        conn.Commit();
    }
    catch (...) {
        conn.Rollback();
        throw;
    }

    SECTION("Result size") {
        REQUIRE(column_result.size() == expected.size());
    }
    SECTION("Result entries") {
        for (int i = 0; auto& entry: column_result) {
            SECTION(std::format("entry#{} (`{}`)", i+1, expected[i].field_name)) {
                SECTION("field name") {
                    CHECK_THAT(entry.field_name, Equals(expected[i].field_name));
                }
                SECTION("field type") {
                    CHECK_THAT(entry.field_type, Equals(expected[i].field_type));
                }
                SECTION("nullable") {
                    CHECK(entry.nullable == expected[i].nullable);
                }
            }
            ++i;
        }
    }
}

static auto runResolveColumnTypeForOtherStatement(LogicalOperatorRef& op, BoundTableRef&& table_ref, StatementType stmt_type) -> void {
    auto column_result = resolveColumnType(op, std::move(table_ref), stmt_type);

    SECTION("Resolve result (NOT generate select list)") {
        REQUIRE(column_result.size() == 0);
    }
}

TEST_CASE("Insert Statement") {
    std::string sql("insert into Foo values (42, 1, null, 'misc...')");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    runBindStatement(sql, {schema}, {});
}

TEST_CASE("Update Statement") {
    std::string sql("update Foo set kind = 2, xys = 101 where id = 42");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    runBindStatement(sql, {schema}, {});
}

TEST_CASE("Delete Statement") {
    std::string sql("delete from Foo where id = 42");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    runBindStatement(sql, {schema}, {});
}

TEST_CASE("Select list only#1") {
    std::string sql("select 123 as a, 98765432100 as b, 'abc' as c");
    std::vector<ColumnEntry> expected{
        {.field_name = "a", .field_type = "INTEGER", .nullable = false},
        {.field_name = "b", .field_type = "BIGINT", .nullable = false},
        {.field_name = "c", .field_type = "VARCHAR", .nullable = false},
    };

    runBindStatement(sql, {}, expected);
}

TEST_CASE("Select constant list only#2 (without alias)") {
    std::string sql("select 123, 98765432100, 'abc'");
    std::vector<ColumnEntry> expected{
        {.field_name = "123", .field_type = "INTEGER", .nullable = false},
        {.field_name = "98765432100", .field_type = "BIGINT", .nullable = false},
        {.field_name = "'abc'", .field_type = "VARCHAR", .nullable = false},
    };

    runBindStatement(sql, {}, expected);
}

TEST_CASE("Select list only with null#1") {
    std::string sql("select 123 as a, 98765432100 as b, null::date as c");
    std::vector<ColumnEntry> expected{
        {.field_name = "a", .field_type = "INTEGER", .nullable = false},
        {.field_name = "b", .field_type = "BIGINT", .nullable = false},
        {.field_name = "c", .field_type = "DATE", .nullable = true},
    };

    runBindStatement(sql, {}, expected);
}

TEST_CASE("Select list only with null#2") {
    std::string sql("select 123 + null as a, 98765432100 as b, 'abc' || null as c");
    std::vector<ColumnEntry> expected{
        {.field_name = "a", .field_type = "INTEGER", .nullable = true},
        {.field_name = "b", .field_type = "BIGINT", .nullable = false},
        {.field_name = "c", .field_type = "VARCHAR", .nullable = true},
    };

    runBindStatement(sql, {}, expected);
}

TEST_CASE("Select list only with null#3") {
    std::string sql("select (null) is not false as a, null is null as b, null is not null as c");
    std::vector<ColumnEntry> expected{
        {.field_name = "a", .field_type = "BOOLEAN", .nullable = false},
        {.field_name = "b", .field_type = "BOOLEAN", .nullable = false},
        {.field_name = "c", .field_type = "BOOLEAN", .nullable = false},
    };

    runBindStatement(sql, {}, expected);
}

TEST_CASE("Select list only with coalesce#1") {
    std::string sql("select coalesce(null, null, 10) as a");
    std::vector<ColumnEntry> expected{
        {.field_name = "a", .field_type = "INTEGER", .nullable = false},
    };

    runBindStatement(sql, {}, expected);
}

TEST_CASE("Select list only with coalesce#2") {
    std::string sql("select coalesce(null, null, null)::VARCHAR as a");
    std::vector<ColumnEntry> expected{
        {.field_name = "a", .field_type = "VARCHAR", .nullable = true},
    };

    runBindStatement(sql, {}, expected);
}

TEST_CASE("Select list only with coalesce#3") {
    std::string sql("select coalesce(null, 42, null) as a");
    std::vector<ColumnEntry> expected{
        {.field_name = "a", .field_type = "INTEGER", .nullable = false},
    };

    runBindStatement(sql, {}, expected);
}

TEST_CASE("Select list only with unary op") {
    std::string sql("select -42 as a");
    std::vector<ColumnEntry> expected{
        {.field_name = "a", .field_type = "INTEGER", .nullable = false},
    };

    runBindStatement(sql, {}, expected);
}

TEST_CASE("Select list only with unary op with null") {
    std::string sql("select -(null)::int as a");
    std::vector<ColumnEntry> expected{
        {.field_name = "a", .field_type = "INTEGER", .nullable = true},
    };

    runBindStatement(sql, {}, expected);
}

TEST_CASE("Select list only with scalar function call") {
    std::string sql("select concat('hello ', 'world ') as fn");
    std::vector<ColumnEntry> expected{
        {.field_name = "fn", .field_type = "VARCHAR", .nullable = true},
    };

    runBindStatement(sql, {}, expected);
}

TEST_CASE("Parameter select list#1") {
    auto sql = std::string(R"#(SELECT CAST($1 AS INTEGER) AS a, CAST($2 AS VARCHAR) AS "CAST($v AS VARCHAR)")#");

    std::vector<ColumnEntry> expected{
        {.field_name = "a", .field_type = "INTEGER", .nullable = true},
        {.field_name = "CAST($v AS VARCHAR)", .field_type = "VARCHAR", .nullable = true},
    };

    runBindStatement(sql, {}, expected);
}

TEST_CASE("Select case expr#1") {
    std::string sql(R"#(
        select (case when kind > 0 then kind else -kind end) as xyz from Foo
    )#");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    std::vector<ColumnEntry> expected{
        {.field_name = "xyz", .field_type = "INTEGER", .nullable = false},
    };

    runBindStatement(sql, {schema}, expected);
}

TEST_CASE("Select case expr#2") {
    std::string sql(R"#(
        select (case kind when 0 then id + 10000 else id end) as xyz from Foo
    )#");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    std::vector<ColumnEntry> expected{
        {.field_name = "xyz", .field_type = "INTEGER", .nullable = false},
    };

    runBindStatement(sql, {schema}, expected);
}

TEST_CASE("Select case expr#3") {
    std::string sql(R"#(
        select (case when kind > 0 then xys else xys end) as xyz from Foo
    )#");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    std::vector<ColumnEntry> expected{
        {.field_name = "xyz", .field_type = "INTEGER", .nullable = true},
    };

    runBindStatement(sql, {schema}, expected);
}

TEST_CASE("Select case expr#3 (without else)") {
    std::string sql(R"#(
        select (case when kind > 0 then kind end) as xyz from Foo
    )#");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    std::vector<ColumnEntry> expected{
        {.field_name = "xyz", .field_type = "INTEGER", .nullable = true},
    };

    runBindStatement(sql, {schema}, expected);
}

TEST_CASE("Select from table#1 (with star expr)") {
    std::string sql("select * from Foo");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    std::vector<ColumnEntry> expected{
        {.field_name = "id", .field_type = "INTEGER", .nullable = false},
        {.field_name = "kind", .field_type = "INTEGER", .nullable = false},
        {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
        {.field_name = "remarks", .field_type = "VARCHAR", .nullable = true},
    };

    runBindStatement(sql, {schema}, expected);
}

TEST_CASE("Select from table#2") {
    std::string sql("select kind, xys from Foo");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    std::vector<ColumnEntry> expected{
        {.field_name = "kind", .field_type = "INTEGER", .nullable = false},
        {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
    };

    runBindStatement(sql, {schema}, expected);
}

TEST_CASE("Select from table#3 (unordered column)") {
    std::string sql("select kind, xys, id from Foo");
    std::string schema("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");

    std::vector<ColumnEntry> expected{
        {.field_name = "kind", .field_type = "INTEGER", .nullable = false},
        {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
        {.field_name = "id", .field_type = "INTEGER", .nullable = false},
    };

    runBindStatement(sql, {schema}, expected);
}

TEST_CASE("Select from joined table#2") {
    std::string sql(R"#(
        select Foo.id, Foo.remarks, Bar.value
        from Foo 
        join Bar on Foo.id = Bar.id and Bar.value <> $2
        where Foo.kind = $3
    )#");
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");

    std::vector<ColumnEntry> expected{
        {.field_name = "id", .field_type = "INTEGER", .nullable = false},
        {.field_name = "remarks", .field_type = "VARCHAR", .nullable = true},
        {.field_name = "value", .field_type = "VARCHAR", .nullable = false},
    };

    runBindStatement(sql, {schema_1, schema_2}, expected);
}

TEST_CASE("Select from joined table#3 (unordered columns)") {
    std::string sql(R"#(
        select Foo.id, Bar.value, Foo.remarks
        from Foo 
        join Bar on Foo.id = Bar.id and Bar.value <> $2
        where Foo.kind = $3
    )#");
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");

    std::vector<ColumnEntry> expected{
        {.field_name = "id", .field_type = "INTEGER", .nullable = false},
        {.field_name = "value", .field_type = "VARCHAR", .nullable = false},
        {.field_name = "remarks", .field_type = "VARCHAR", .nullable = true},
    };

    runBindStatement(sql, {schema_1, schema_2}, expected);
}

TEST_CASE("Select from joined table#4 (with star expr)") {
    std::string sql(R"#(
        select *
        from Foo 
        join Bar on Foo.id = Bar.id and Bar.value <> $2
        where Foo.kind = $3
    )#");
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");

    std::vector<ColumnEntry> expected{
        {.field_name = "id", .field_type = "INTEGER", .nullable = false},
        {.field_name = "kind", .field_type = "INTEGER", .nullable = false},
        {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
        {.field_name = "remarks", .field_type = "VARCHAR", .nullable = true},
        {.field_name = "id_1", .field_type = "INTEGER", .nullable = false},
        {.field_name = "value", .field_type = "VARCHAR", .nullable = false},
    };

    runBindStatement(sql, {schema_1, schema_2}, expected);
}

TEST_CASE("Select from joined table#5 (with star expr partially)") {
    std::string sql(R"#(
        select Foo.id, Bar.*
        from Foo 
        join Bar on Foo.id = Bar.id and Bar.value <> $2
        where Foo.kind = $3
    )#");
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");

    std::vector<ColumnEntry> expected{
        {.field_name = "id", .field_type = "INTEGER", .nullable = false},
        {.field_name = "id_1", .field_type = "INTEGER", .nullable = false},
        {.field_name = "value", .field_type = "VARCHAR", .nullable = false},
    };

    runBindStatement(sql, {schema_1, schema_2}, expected);
}

TEST_CASE("Select from joined table#6 (with left join)") {
    std::string sql(R"#(
        select *
        from Foo 
        left outer join Bar on Foo.id = Bar.id and Bar.value <> $2
        where Foo.kind = $3
    )#");
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");

    std::vector<ColumnEntry> expected{
        {.field_name = "id", .field_type = "INTEGER", .nullable = false},
        {.field_name = "kind", .field_type = "INTEGER", .nullable = false},
        {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
        {.field_name = "remarks", .field_type = "VARCHAR", .nullable = true},
        {.field_name = "id_1", .field_type = "INTEGER", .nullable = true},
        {.field_name = "value", .field_type = "VARCHAR", .nullable = true},
    };

    runBindStatement(sql, {schema_1, schema_2}, expected);
}

TEST_CASE("Select from joined table#7 (with right join)") {
    std::string sql(R"#(
        select *
        from Foo 
        right join Bar on Foo.id = Bar.id and Bar.value <> $2
        where Foo.kind = $3
    )#");
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");

    std::vector<ColumnEntry> expected{
        {.field_name = "id", .field_type = "INTEGER", .nullable = true},
        {.field_name = "kind", .field_type = "INTEGER", .nullable = true},
        {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
        {.field_name = "remarks", .field_type = "VARCHAR", .nullable = true},
        {.field_name = "id_1", .field_type = "INTEGER", .nullable = false},
        {.field_name = "value", .field_type = "VARCHAR", .nullable = false},
    };

    runBindStatement(sql, {schema_1, schema_2}, expected);
}

TEST_CASE("Select from joined table#8 (with full outer join)") {
    std::string sql(R"#(
        select *
        from Foo 
        full outer join Bar on Foo.id = Bar.id and Bar.value <> $2
        where Foo.kind = $3
    )#");
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");

    std::vector<ColumnEntry> expected{
        {.field_name = "id", .field_type = "INTEGER", .nullable = true},
        {.field_name = "kind", .field_type = "INTEGER", .nullable = true},
        {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
        {.field_name = "remarks", .field_type = "VARCHAR", .nullable = true},
        {.field_name = "id_1", .field_type = "INTEGER", .nullable = true},
        {.field_name = "value", .field_type = "VARCHAR", .nullable = true},
    };

    runBindStatement(sql, {schema_1, schema_2}, expected);
}

TEST_CASE("Select from joined table#9 (with cross join)") {
    std::string sql(R"#(
        select *
        from Foo 
        cross join Bar
        where Foo.kind = $3
    )#");
    std::string schema_1("CREATE TABLE Foo (id int primary key, kind int not null, xys int, remarks VARCHAR)");
    std::string schema_2("CREATE TABLE Bar (id int primary key, value VARCHAR not null)");

    std::vector<ColumnEntry> expected{
        {.field_name = "id", .field_type = "INTEGER", .nullable = false},
        {.field_name = "kind", .field_type = "INTEGER", .nullable = false},
        {.field_name = "xys", .field_type = "INTEGER", .nullable = true},
        {.field_name = "remarks", .field_type = "VARCHAR", .nullable = true},
        {.field_name = "id_1", .field_type = "INTEGER", .nullable = false},
        {.field_name = "value", .field_type = "VARCHAR", .nullable = false},
    };

    runBindStatement(sql, {schema_1, schema_2}, expected);
}

TEST_CASE("Select from scalar subquery") {
    SKIP("Not implemented");
}

TEST_CASE("Select from derived table") {
    SKIP("Not implemented");
}

TEST_CASE("Select from derived table join") {
    SKIP("Not implemented");
}

TEST_CASE("Select from table function") {
    SKIP("Not implemented");
}

#endif