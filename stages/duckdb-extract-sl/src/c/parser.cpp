#include <iomanip>

#include <duckdb.hpp>
#include <duckdb/parser/query_node/list.hpp>
#include <duckdb/parser/tableref/list.hpp>
#include <duckdb/parser/expression/list.hpp>

#define MAGIC_ENUM_RANGE_MAX (std::numeric_limits<uint8_t>::max())

#include <magic_enum/magic_enum.hpp>
#include <zmq.h>

#include "cbor_encode.hpp"
#include "duckdb_database.hpp"
#include "duckdb_worker.h"

#include <iostream>
#include <format>

// #include "yyjson.hpp"
// #include "json_serializer.hpp"

class SelectListCollector {
public:
    SelectListCollector(worker::Database *db, std::string id, std::optional<void *> socket) 
        : conn(std::move(db->connect())), id(id), socket(socket) {}
public:
    auto execute(std::string query) -> WorkerResultCode;
public:
    auto warn(const std::string& message) -> void;
    auto err(const std::string& message) -> void;
private:
    duckdb::Connection conn;
    std::string id;
    std::optional<void *> socket;
private:
    friend auto borrowConnection(SelectListCollector& collector) -> duckdb::Connection&;
};

struct DescribeResult {
    std::string field_name;
    std::string field_type;
    bool nullable;
};

struct WorkerResult {
    std::string id;
    std::size_t stmt_offset;
    std::size_t stmt_count;
    std::vector<DescribeResult> describes;
};

auto borrowConnection(SelectListCollector& collector) -> duckdb::Connection& {
    return collector.conn;
}

static auto prependDescribeKeyword(duckdb::SelectStatement& stmt) -> void {
    auto& original_node = stmt.node;

    auto describe = new duckdb::ShowRef();
    describe->show_type = duckdb::ShowType::DESCRIBE;
    describe->query = std::move(original_node);

    auto describe_node = new duckdb::SelectNode();
    describe_node->from_table.reset(describe);
    describe_node->select_list.push_back(duckdb::StarExpression().Copy());

    stmt.node.reset(describe_node);

    stmt.named_param_map = {{"1", 0}};
    stmt.n_param = 1;
}

static auto hydrateDescribeResult(const SelectListCollector& collector, const duckdb::unique_ptr<duckdb::QueryResult>& query_result, std::vector<DescribeResult>& results) -> void {
    for (auto& row: *query_result) {
        // TODO: dealing with untype

        results.push_back({
            row.GetValue<std::string>(0),
            row.GetValue<std::string>(1),
            row.GetValue<std::string>(2) == "YES",
        });
    }
}

static auto describeSelectStatementInternal(SelectListCollector& collector, const duckdb::SelectStatement& stmt, std::vector<DescribeResult>& results) -> WorkerResultCode {
    auto& conn = borrowConnection(collector);

    std::string err_message;
    WorkerResultCode err;

    try {
        std::cout << std::format("Q: {}, p: {}", stmt.ToString(), stmt.Copy()->named_param_map[std::string("1")]) << std::endl;
        // auto prepares_stmt = std::move(conn.Prepare(stmt.Copy()));
        auto prepares_stmt = std::move(conn.Prepare(stmt.ToString()));
        // passes null value to all parameter(s)
        auto param_count = prepares_stmt->GetStatementProperties().parameter_count;
        auto params = duckdb::vector<duckdb::Value>(param_count);

        auto query_result = prepares_stmt->Execute(params);
        ::hydrateDescribeResult(collector, query_result, results);
        return no_error;
    }
    catch (const duckdb::ParameterNotResolvedException& ex) {
        err_message = "Cannot infer type for untyped placeholder";
        err = describe_filed;
    }
    catch (const duckdb::Exception& ex) {
        err_message = ex.what();
        err = invalid_sql;
    }

    collector.err(err_message);

    return err;
}

static auto keepColumnName(duckdb::unique_ptr<duckdb::ParsedExpression>& expr) -> std::string {
    if (expr->alias != "") {
        return expr->alias;
    }
    else {
        return expr->ToString();
    }
}

static auto walkOrderBysNodeInternal(SelectListCollector& collector, duckdb::unique_ptr<duckdb::OrderModifier>& order_bys, uint32_t depth) -> void;
static auto walkSelectStatementInternal(SelectListCollector& collector, duckdb::SelectStatement& stmt, uint32_t depth) -> void;

static auto walkExpressionInternal(SelectListCollector& collector, duckdb::unique_ptr<duckdb::ParsedExpression>& expr, uint32_t depth) -> void {
    switch (expr->expression_class) {
    case duckdb::ExpressionClass::PARAMETER:
        {
            auto& param_expr = expr->Cast<duckdb::ParameterExpression>();
            param_expr.identifier = "1";
        }
        break;
    case duckdb::ExpressionClass::CAST: 
        {
            auto& cast_expr = expr->Cast<duckdb::CastExpression>();
            ::walkExpressionInternal(collector, cast_expr.child, depth+1);
        }
        break;
    case duckdb::ExpressionClass::COMPARISON:
        {
            auto& cmp_expr = expr->Cast<duckdb::ComparisonExpression>();
            ::walkExpressionInternal(collector, cmp_expr.left, depth+1);
            ::walkExpressionInternal(collector, cmp_expr.right, depth+1);
        }
        break;
    case duckdb::ExpressionClass::BETWEEN:
        {
            auto& between_expr = expr->Cast<duckdb::BetweenExpression>();
            ::walkExpressionInternal(collector, between_expr.input, depth+1);
            ::walkExpressionInternal(collector, between_expr.lower, depth+1);
            ::walkExpressionInternal(collector, between_expr.upper, depth+1);
        }
        break;
    case duckdb::ExpressionClass::CASE:
        {
            auto& case_expr = expr->Cast<duckdb::CaseExpression>();

            // whrn-then clause
            for (auto& case_check: case_expr.case_checks) {
                ::walkExpressionInternal(collector, case_check.when_expr, depth+1);
                ::walkExpressionInternal(collector, case_check.then_expr, depth+1);
            }
            
            // else clause
            ::walkExpressionInternal(collector, case_expr.else_expr, depth+1);
        }
        break;
    case duckdb::ExpressionClass::CONJUNCTION:
        {
            auto& conj_expr = expr->Cast<duckdb::ConjunctionExpression>();
            
            for (auto& child: conj_expr.children) {
                ::walkExpressionInternal(collector, child, depth+1);
            }
        }
        break;
    case duckdb::ExpressionClass::FUNCTION:
        {
            auto& fn_expr = expr->Cast<duckdb::FunctionExpression>();

            for (auto& child: fn_expr.children) {
                ::walkExpressionInternal(collector, child, depth+1);
            }

            // order by(s)
            ::walkOrderBysNodeInternal(collector, fn_expr.order_bys, depth+1);
        }
        break;
    case duckdb::ExpressionClass::SUBQUERY:
        {
            auto& sq_expr = expr->Cast<duckdb::SubqueryExpression>();
            ::walkSelectStatementInternal(collector, *sq_expr.subquery, depth+1);

            if (sq_expr.subquery_type == duckdb::SubqueryType::ANY) {
                ::walkExpressionInternal(collector, sq_expr.child, depth+1);
            }
        }
        break;
    case duckdb::ExpressionClass::CONSTANT:
    case duckdb::ExpressionClass::COLUMN_REF:
        // no conversion
        break;
    case duckdb::ExpressionClass::OPERATOR:
    default: 
        collector.warn(std::format("[TODO] Unsupported expression class: {}", magic_enum::enum_name(expr->expression_class)));
        break;
    }
}

static auto walkOrderBysNodeInternal(SelectListCollector& collector, duckdb::unique_ptr<duckdb::OrderModifier>& order_bys, uint32_t depth) -> void {
    for (auto& order_by: order_bys->orders) {
        ::walkExpressionInternal(collector, order_by.expression, depth);
    }
}

static auto walkSelectListItem(SelectListCollector& collector, duckdb::unique_ptr<duckdb::ParsedExpression>& expr, uint32_t depth) -> void {
    if (expr->HasParameter() || expr->HasSubquery()) {
        if (depth > 0) {
            ::walkExpressionInternal(collector, expr, depth);
        }
        else {
            auto new_alias = keepColumnName(expr);

            ::walkExpressionInternal(collector, expr, depth);

            if (expr->ToString() != new_alias) {
                expr->alias = new_alias;
            }

            // TODO: record untyped patameter column (untyped == no type casting)

        }
    }
}

static auto walkTableRef(SelectListCollector& collector, duckdb::unique_ptr<duckdb::TableRef>& table_ref, uint32_t depth) -> void {
    switch (table_ref->type) {
    case duckdb::TableReferenceType::BASE_TABLE:
    case duckdb::TableReferenceType::EMPTY_FROM:
        // no conversion
        break;
    case duckdb::TableReferenceType::TABLE_FUNCTION:
        {
            auto& table_fn = table_ref->Cast<duckdb::TableFunctionRef>();
            ::walkExpressionInternal(collector, table_fn.function, depth);
        }
        break;
    case duckdb::TableReferenceType::JOIN:
        {
            auto& join_ref = table_ref->Cast<duckdb::JoinRef>();
            ::walkTableRef(collector, join_ref.left, depth+1);
            ::walkTableRef(collector, join_ref.right, depth+1);
            ::walkExpressionInternal(collector, join_ref.condition, depth+1);
        }
        break;
    case duckdb::TableReferenceType::SUBQUERY:
        {
            auto& sq_ref = table_ref->Cast<duckdb::SubqueryRef>();
            ::walkSelectStatementInternal(collector, *sq_ref.subquery, 0);
        }
        break;
    default:
        collector.warn(std::format("[TODO] Unsupported table ref type: {}", magic_enum::enum_name(table_ref->type)));
        break;
    }
}

static auto walkSelectStatementInternal(SelectListCollector& collector, duckdb::SelectStatement& stmt, uint32_t depth) -> void {
    switch (stmt.node->type) {
    case duckdb::QueryNodeType::SELECT_NODE: 
        {
            auto& select_node =  stmt.node->Cast<duckdb::SelectNode>();
            for (auto& expr: select_node.select_list) {
                ::walkSelectListItem(collector, expr, depth);
            }
            form_clause: {
                ::walkTableRef(collector, select_node.from_table, depth+1);
            }
            if (select_node.where_clause) {
                ::walkExpressionInternal(collector, select_node.where_clause, depth+1);
            }
            if (select_node.groups.group_expressions.size() > 0) {
                collector.warn(std::format("[TODO] Unsupported group by clause"));
            }
            if (select_node.having) {
                collector.warn(std::format("[TODO] Unsupported having clause"));
            }
            if (select_node.sample) {
                collector.warn(std::format("[TODO] Unsupported sample clause"));
            }
        }
        break;
    default: 
        collector.warn(std::format("[TODO] Unsupported select node: {}", magic_enum::enum_name(stmt.node->type)));
        break;
    }
}

static auto walkSelectStatement(SelectListCollector& collector, duckdb::SelectStatement& stmt) -> void {
    ::walkSelectStatementInternal(collector, stmt, 0);
}

static auto describeSelectStatement(SelectListCollector& collector, duckdb::SelectStatement& stmt, std::vector<DescribeResult>& results) -> WorkerResultCode {
    ::walkSelectStatement(collector, stmt);
    ::prependDescribeKeyword(stmt);
    return ::describeSelectStatementInternal(collector, stmt, results);
}

static auto sendLog(void *socket, const std::string& log_level, const std::string& id, const std::string& message) -> void {
    event_type: {
        auto event_type = std::string("worker_result");
        ::zmq_send(socket, event_type.data(), event_type.length(), ZMQ_SNDMORE);    
    }
    from: {
        auto from = std::string("extract-task");
        zmq_send(socket, from.data(), from.length(), ZMQ_SNDMORE);    
    }
    payload: {
        CborEncoder payload_encoder;

        event_tag: {
            payload_encoder.addString("log");
        }
        source_path: {
            payload_encoder.addString(id);
        }
        log_level: {
            payload_encoder.addString(log_level);
        }
        log_content: {
            payload_encoder.addString(std::format("{} ({})", message, id));
        }

        auto encode_result = payload_encoder.build();
        zmq_send(socket, encode_result.data(), encode_result.size(), ZMQ_SNDMORE);    
        zmq_send(socket, "", 0, 0);    
    }
}

static auto encodeDescribeResult(const std::vector<DescribeResult>& describes) -> std::vector<char> {
    CborEncoder encoder;

    encoder.addArrayHeader(describes.size());

    for (auto& desc: describes) {
        encoder.addArrayHeader(3);
        encoder.addString(desc.field_name);
        encoder.addString(desc.field_type);
        encoder.addBool(desc.nullable);
    }

    return std::move(encoder.rawBuffer());
}

static auto sendWorkerResult(const std::optional<void *>& socket_, int32_t index, const WorkerResult& result) -> void {
    if (!socket_) return;

    auto socket = socket_.value();

    event_type: {
        auto event_type = std::string("worker_result");
        zmq_send(socket, event_type.data(), event_type.length(), ZMQ_SNDMORE);    
    }
    from: {
        auto from = std::string("extract-task");
        zmq_send(socket, from.data(), from.length(), ZMQ_SNDMORE);    
    }
    payload: {
        std::vector<char> buf;

        CborEncoder payload_encoder;
        result_tag: {
            payload_encoder.addString("topic_body");
        }
        work_id: {
            payload_encoder.addString(result.id);
        }
        stmt_count: {
            payload_encoder.addUInt(result.stmt_count);
        }
        stmt_offset: {
            payload_encoder.addUInt(result.stmt_offset);
        }
        topic_body: {
            payload_encoder.addBinaryPair(topic_select_list, encodeDescribeResult(result.describes));
        }

        auto encode_result = payload_encoder.build();

        zmq_send(socket, encode_result.data(), encode_result.size(), ZMQ_SNDMORE);
        zmq_send(socket, "", 0, 0);    
    }
}

auto SelectListCollector::execute(std::string query) -> WorkerResultCode {
    std::string message;
    try {
        auto stmts = this->conn.ExtractStatements(query);

        // for (auto& stmt: stmts) {
        if (stmts.size() > 0) {
            auto& stmt = stmts[0];
            const int32_t index = 1;

            std::vector<DescribeResult> describes;

            if (stmt->type == duckdb::StatementType::SELECT_STATEMENT) {
                describeSelectStatement(*this, stmt->Cast<duckdb::SelectStatement>(), describes);
            }

            WorkerResult result = {this->id, 1, stmts.size(), std::move(describes)};
            // send as worker result
            ::sendWorkerResult(this->socket, 1, result);
        }

        return no_error;
    }
    catch (const duckdb::ParserException& ex) {
        message = ex.what();
    }
    
    this->err(message);
    return invalid_sql;
}

auto SelectListCollector::warn(const std::string& message) -> void {
    if (this->socket) {
        ::sendLog(this->socket.value(), "warn", this->id, message);
    }
    else {
        std::cout << std::format("warn: {}", message) << std::endl;
    }
}
auto SelectListCollector::err(const std::string& message) -> void {
    if (this->socket) {
        ::sendLog(this->socket.value(), "err", this->id, message);
    }
    else {
        std::cout << std::format("err: {}", message) << std::endl;
    }
}


extern "C" {
    auto initCollector(DatabaseRef db_ref, const char *id, size_t id_len, void *socket, CollectorRef *handle) -> int32_t {
        auto db = reinterpret_cast<worker::Database *>(db_ref);
        auto collector = new SelectListCollector(db, std::string(id, id_len), socket ? std::make_optional(socket) : std::nullopt);

        *handle = reinterpret_cast<CollectorRef>(collector);
        return 0;
    }

    auto deinitCollector(CollectorRef handle) -> void {
        delete reinterpret_cast<SelectListCollector *>(handle);
    }

    auto executeDescribe(CollectorRef handle, const char *query, size_t query_len) -> void {
        auto collector = reinterpret_cast<SelectListCollector *>(handle);

        collector->execute(std::string(query, query_len));
    }

    auto ParseDescribeStmt() -> void {
        auto sql = "describe select $a::int, xyz, 123 from Foo";

        auto db = duckdb::DuckDB(nullptr);
        auto conn = duckdb::Connection(db);

        auto stmts = conn.ExtractStatements(sql);
        auto& stmt = stmts[0];

        std::cout << std::format("stmt/type: {}", magic_enum::enum_name(stmt->type)) << std::endl;

        duckdb::SelectStatement& select_stmt = stmt->Cast<duckdb::SelectStatement>();

        std::cout << std::format("node/type: {} ({})", magic_enum::enum_name(select_stmt.node->type), select_stmt.node->type == duckdb::QueryNodeType::SELECT_NODE) << std::endl;
        std::cout << std::format("node/type#2: {}", duckdb::QueryNodeType::SELECT_NODE == duckdb::SelectNode::TYPE) << std::endl;

        auto& root_node = select_stmt.node->Cast<duckdb::SelectNode>();

        std::cout << std::format("select-node/list: {}", root_node.select_list.size()) << std::endl;
        std::cout << std::format("table-ref/type: {}", magic_enum::enum_name(root_node.from_table->type)) << std::endl;

        auto& table_ref = root_node.from_table->Cast<duckdb::ShowRef>();

        std::cout << std::format("showref/type: {} name: {}, node?: {}", 
            magic_enum::enum_name(table_ref.show_type), 
            table_ref.table_name == "" ? "<show query>" : table_ref.table_name, 
            (bool)table_ref.query
        ) << std::endl;

        std::cout << std::format("query: {}", table_ref.ToString()) << std::endl;
    }
}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

using namespace Catch::Matchers;

TEST_CASE("Error SQL") {
    auto sql = std::string("SELT $1::int as a");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);
    
    auto err = collector.execute(sql);

    SECTION("execute result code") {
        CHECK(err != 0);
    }
}

TEST_CASE("Convert positional parameter") {
    auto sql = std::string("select $1 as a from Foo");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals("SELECT $1 AS a FROM Foo"));
    }
}

TEST_CASE("Convert positional parameter without alias") {
    auto sql = std::string("select $1 from Foo");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals("SELECT $1 FROM Foo"));
    }
}

TEST_CASE("Convert named parameter") {
    auto sql = std::string("select $value as a from Foo");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals("SELECT $1 AS a FROM Foo"));
    }
}

TEST_CASE("Convert named parameter with type cast") {
    auto sql = std::string("select $value::int as a from Foo");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals("SELECT CAST($1 AS INTEGER) AS a FROM Foo"));
    }
}

TEST_CASE("Convert named parameter without alias") {
    auto sql = std::string("select $v, v from Foo");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals(R"(SELECT $1 AS "$v", v FROM Foo)"));
    }
}

TEST_CASE("Convert named parameter with type cast without alias") {
    auto sql = std::string("select $user_name::text, user_name from Foo");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals(R"#(SELECT CAST($1 AS VARCHAR) AS "CAST($user_name AS VARCHAR)", user_name FROM Foo)#"));
    }
}

TEST_CASE("Convert named parameter as expr without alias") {
    auto sql = std::string("select $x::int + $y::int from Foo");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals(R"#(SELECT (CAST($1 AS INTEGER) + CAST($1 AS INTEGER)) AS "(CAST($x AS INTEGER) + CAST($y AS INTEGER))" FROM Foo)#"));
    }
}

TEST_CASE("Convert named parameter inslude betteen without alias") {
    auto sql = std::string("select $v::int between $x::int and $y::int from Foo");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals(R"#(SELECT (CAST($1 AS INTEGER) BETWEEN CAST($1 AS INTEGER) AND CAST($1 AS INTEGER)) AS "(CAST($v AS INTEGER) BETWEEN CAST($x AS INTEGER) AND CAST($y AS INTEGER))" FROM Foo)#"));
    }
}

TEST_CASE("Convert named parameter inslude case expr without alias#1") {
    // case
    auto sql = std::string("select case when $v::int = 0 then $x::int else $y::int end from Foo");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals(R"#(SELECT CASE  WHEN ((CAST($1 AS INTEGER) = 0)) THEN (CAST($1 AS INTEGER)) ELSE CAST($1 AS INTEGER) END AS "CASE  WHEN ((CAST($v AS INTEGER) = 0)) THEN (CAST($x AS INTEGER)) ELSE CAST($y AS INTEGER) END" FROM Foo)#"));
    }
}

TEST_CASE("Convert named parameter inslude case expr without alias#2") {
    // case
    auto sql = std::string("select case $v::int when 99 then $x::int else $y::int end from Foo");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals(R"#(SELECT CASE  WHEN ((CAST($1 AS INTEGER) = 99)) THEN (CAST($1 AS INTEGER)) ELSE CAST($1 AS INTEGER) END AS "CASE  WHEN ((CAST($v AS INTEGER) = 99)) THEN (CAST($x AS INTEGER)) ELSE CAST($y AS INTEGER) END" FROM Foo)#"));
    }
}

TEST_CASE("Convert named parameter inslude logical operator without alias") {
    auto sql = std::string("select $x::int = 123 AND $y::text = 'abc' from Foo");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals(R"#(SELECT ((CAST($1 AS INTEGER) = 123) AND (CAST($1 AS VARCHAR) = 'abc')) AS "((CAST($x AS INTEGER) = 123) AND (CAST($y AS VARCHAR) = 'abc'))" FROM Foo)#"));
    }
}

TEST_CASE("Convert named parameter in function args without alias") {
    auto sql = std::string("select string_agg(s, $sep::text) from Foo");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals(R"#(SELECT string_agg(s, CAST($1 AS VARCHAR)) AS "string_agg(s, CAST($sep AS VARCHAR))" FROM Foo)#"));
    }
}

TEST_CASE("Convert named parameter in function args without alias#2") {
    auto sql = std::string("select string_agg(n, $sep::text order by fmod(n, $deg::int) desc) from range(0, 360, 30) t(n)");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals(R"#(SELECT string_agg(n, CAST($1 AS VARCHAR) ORDER BY fmod(n, CAST($1 AS INTEGER)) DESC) AS "string_agg(n, CAST($sep AS VARCHAR) ORDER BY fmod(n, CAST($deg AS INTEGER)) DESC)" FROM range(0, 360, 30) AS t(n))#"));
    }
}

TEST_CASE("Convert named parameter in subquery without alias") {
    auto sql = std::string(R"#(
        select (select Foo.v + Point.x + $offset::int from Point)
        from Foo
    )#");
    auto expect = std::string(R"#(SELECT (SELECT ((Foo.v + Point.x) + CAST($1 AS INTEGER)) FROM Point) AS "(SELECT ((Foo.v + Point.x) + CAST($offset AS INTEGER)) FROM Point)" FROM Foo)#");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals(expect));
    }
}

TEST_CASE("Convert named parameter without alias in exists clause") {
    auto sql = std::string(R"#(
        select exists (select Foo.v - Point.x > $diff::int from Point)
        from Foo
    )#");
    auto expect = std::string(R"#(SELECT EXISTS(SELECT ((Foo.v - Point.x) > CAST($1 AS INTEGER)) FROM Point) AS "EXISTS(SELECT ((Foo.v - Point.x) > CAST($diff AS INTEGER)) FROM Point)" FROM Foo)#");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals(expect));
    }
}

TEST_CASE("Convert named parameter without alias in any clause") {
    auto sql = std::string(R"#(
        select $v::int = any(select * from range(0, 10, $step::int))
    )#");
    auto expect = std::string(R"#(SELECT (CAST($1 AS INTEGER) = ANY(SELECT * FROM range(0, 10, CAST($1 AS INTEGER)))) AS "(CAST($v AS INTEGER) = ANY(SELECT * FROM range(0, 10, CAST($step AS INTEGER))))")#");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals(expect));
    }
}

TEST_CASE("Convert named parameter without alias in any clause#2") {
    auto sql = std::string(R"#(select $v::int = any(range(0, 10, $step::int)))#");
    auto expect = std::string(R"#(SELECT (CAST($1 AS INTEGER) = ANY(SELECT unnest(range(0, 10, CAST($1 AS INTEGER))))) AS "(CAST($v AS INTEGER) = ANY(SELECT unnest(range(0, 10, CAST($step AS INTEGER)))))")#");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals(expect));
    }
}

TEST_CASE("Convert named parameter in table function args") {
    auto sql = std::string("select id * 101 from range(0, 10, $step::int) t(id)");
    auto expect = std::string("SELECT (id * 101) FROM range(0, 10, CAST($1 AS INTEGER)) AS t(id)");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals(expect));
    }
}

TEST_CASE("Convert named parameter in joined condition") {
    auto sql = std::string("select * from Foo join Bar on Foo.v = Bar.v and Bar.x = $x::int");
    auto expect = std::string("SELECT * FROM Foo INNER JOIN Bar ON (((Foo.v = Bar.v) AND (Bar.x = CAST($1 AS INTEGER))))");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals(expect));
    }
}

TEST_CASE("Convert named parameter in derived table (subquery)") {
    auto sql = std::string(R"#(
        select * from (
            select $v::int, $s::text 
        ) v
    )#");
    auto expect = std::string(R"#(SELECT * FROM (SELECT CAST($1 AS INTEGER) AS "CAST($v AS INTEGER)", CAST($1 AS VARCHAR) AS "CAST($s AS VARCHAR)") AS v)#");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals(expect));
    }
}

TEST_CASE("Convert named parameter in where clause") {
    auto sql = std::string(R"#(
        select * from Foo
        where v = $v::int and kind = $k::int
    )#");
    auto expect = std::string("SELECT * FROM Foo WHERE ((v = CAST($1 AS INTEGER)) AND (kind = CAST($1 AS INTEGER)))");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = ::borrowConnection(collector);
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::walkSelectStatement(collector, stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals(expect));
    }
}

TEST_CASE("Convert named parameter ????") {
    // TODO: not implement
    // order by
    // window function
    //    * filter
    //    * partition
    //    * order by
    //    * frame
}//

TEST_CASE("Prepend describe") {
    auto sql = std::string("select $1::int as a, xyz, 123, $2::text as c from Foo");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto conn = db.connect();;
    auto stmts = conn.ExtractStatements(sql);
    auto& stmt = stmts[0];
    ::prependDescribeKeyword(stmt->Cast<duckdb::SelectStatement>());

    SECTION("result") {
        CHECK_THAT(stmt->ToString(), Equals("DESCRIBE (SELECT CAST($1 AS INTEGER) AS a, xyz, 123, CAST($2 AS VARCHAR) AS c FROM Foo)"));
    }
}

TEST_CASE("Not exist relation") {
    auto sql = std::string("SELECT $1::int as p from Origin");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);
    auto stmts = std::move(::borrowConnection(collector).ExtractStatements(sql));

    std::vector<DescribeResult> results;
    ::describeSelectStatement(collector, stmts[0]->Cast<duckdb::SelectStatement>(), results);

    SECTION("describe result") {
        REQUIRE(results.size() == 0);
    }
}

TEST_CASE("Describe SELECT list") {
    auto sql = std::string("SELECT $a::int AS a, 123, $c::text AS c");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);
    auto stmts = std::move(::borrowConnection(collector).ExtractStatements(sql));

    std::vector<DescribeResult> results;
    auto err = ::describeSelectStatement(collector, stmts[0]->Cast<duckdb::SelectStatement>(), results);

    SECTION("describe result") {
        SECTION("err code") {
            REQUIRE(err == no_error);
        }
        SECTION("result count") {
            REQUIRE(results.size() == 3);
        }
        SECTION("row data#1") {
            auto& row = results[0];
            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("a"));
            }
            SECTION("field type") {
                CHECK(row.field_type);
                CHECK_THAT(row.field_type.value(), Equals("INTEGER"));
            }
            SECTION("nullable") {
                CHECK(row.nullable);
            }
        }
        SECTION("row data#2") {
            auto& row = results[1];

            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("123"));
            }
            SECTION("field type") {
                CHECK(row.field_type);
                CHECK_THAT(row.field_type.value(), Equals("INTEGER"));
            }
            SECTION("nullable") {
                CHECK(row.nullable);
            }
        }
        SECTION("row data#3") {
            auto& row = results[2];

            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("c"));
            }
            SECTION("field type") {
                CHECK(row.field_type);
                CHECK_THAT(row.field_type.value(), Equals("VARCHAR"));
            }
            SECTION("nullable") {
                CHECK(row.nullable);
            }
        }
    }
}

TEST_CASE("Describe SELECT list without placeholder alias") {
    auto sql = std::string("SELECT $a::int, 123 as b, $c::text");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);
    auto stmts = std::move(::borrowConnection(collector).ExtractStatements(sql));

    std::vector<DescribeResult> results;
    auto err = ::describeSelectStatement(collector, stmts[0]->Cast<duckdb::SelectStatement>(), results);

    SECTION("describe result") {
        SECTION("err code") {
            REQUIRE(err == no_error);
        }
        SECTION("result count") {
            REQUIRE(results.size() == 3);
        }
        SECTION("row data#1") {
            auto& row = results[0];

            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("CAST($a AS INTEGER)"));
            }
            SECTION("field type") {
                CHECK(row.field_type);
                CHECK_THAT(row.field_type.value(), Equals("INTEGER"));
            }
            SECTION("nullable") {
                CHECK(row.nullable);
            }
        }
        SECTION("row data#2") {
            auto& row = results[1];

            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("b"));
            }
            SECTION("field type") {
                CHECK(row.field_type);
                CHECK_THAT(row.field_type.value(), Equals("INTEGER"));
            }
            SECTION("nullable") {
                CHECK(row.nullable);
            }
        }
        SECTION("row data#3") {
            auto& row = results[2];

            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("CAST($c AS VARCHAR)"));
            }
            SECTION("field type") {
                CHECK(row.field_type);
                CHECK_THAT(row.field_type.value(), Equals("VARCHAR"));
            }
            SECTION("nullable") {
                CHECK(row.nullable);
            }
        }
    }
}

TEST_CASE("Describe SELECT list with not null definition") {
    SKIP("(TODO) Need handling nullability manually"); 

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);

    auto& conn = borrowConnection(collector);
    conn.Query("CREATE TABLE Foo (x INTEGER NOT NULL, v VARCHAR)");

    auto sql = std::string("SELECT x, v FROM Foo WHERE x = $x");
    auto stmts = std::move(::borrowConnection(collector).ExtractStatements(sql));

    std::vector<DescribeResult> results;
    auto err = ::describeSelectStatement(collector, stmts[0]->Cast<duckdb::SelectStatement>(), results);

    SECTION("describe result") {
        SECTION("err code") {
            REQUIRE(err == no_error);
        }
        SECTION("result count") {
            REQUIRE(results.size() == 2);
        }
        SECTION("row data#1") {
            auto& row = results[0];

            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("x"));
            }
            SECTION("field type") {
                CHECK(row.field_type);
                CHECK_THAT(row.field_type.value(), Equals("INTEGER"));
            }
            SECTION("nullable") {
                CHECK_FALSE(row.nullable);
            }
        }
        SECTION("row data#2") {
            auto& row = results[1];

            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("v"));
            }
            SECTION("field type") {
                CHECK(row.field_type);
                CHECK_THAT(row.field_type.value(), Equals("VARCHAR"));
            }
            SECTION("nullable") {
                CHECK(row.nullable);
            }
        }
    }  
}

TEST_CASE("Describe SELECT list with untyped placeholder#1") {
    auto sql = std::string("SELECT $a || $b::text, 123 as b");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);
    auto stmts = std::move(::borrowConnection(collector).ExtractStatements(sql));

    std::vector<DescribeResult> results;
    auto err = ::describeSelectStatement(collector, stmts[0]->Cast<duckdb::SelectStatement>(), results);

    SECTION("describe result") {
        SECTION("err code") {
            REQUIRE(err == no_error);
        }
        SECTION("result count") {
            REQUIRE(results.size() == 2);
        }
        SECTION("row data#1") {
            auto& row = results[0];

            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("($a || CAST($b AS VARCHAR))"));
            }
            SECTION("field type") {
                CHECK(row.field_type);
                CHECK_THAT(row.field_type.value(), Equals("VARCHAR"));
            }
            SECTION("nullable") {
                CHECK(row.nullable);
            }
        }
        SECTION("row data#2") {
            auto& row = results[1];

            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("b"));
            }
            SECTION("field type") {
                CHECK(row.field_type);
                CHECK_THAT(row.field_type.value(), Equals("INTEGER"));
            }
            SECTION("nullable") {
                CHECK(row.nullable);
            }
        }
    }
}

TEST_CASE("Describe SELECT list with untyped placeholder#2") {
    auto sql = std::string("SELECT v || $a, 123 as b FROM Foo");

    auto db = worker::Database();
    auto collector = SelectListCollector(&db, "1", std::nullopt);
    borrowConnection(collector).Query("CREATE TABLE Foo (x INTEGER NOT NULL, v VARCHAR)");
    auto stmts = std::move(::borrowConnection(collector).ExtractStatements(sql));

    std::vector<DescribeResult> results;
    auto err = ::describeSelectStatement(collector, stmts[0]->Cast<duckdb::SelectStatement>(), results);

    SECTION("describe result") {
        SECTION("err code") {
            REQUIRE(err == no_error);
        }
        SECTION("result count") {
            REQUIRE(results.size() == 2);
        }
        SECTION("row data#1") {
            auto& row = results[0];

            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("(v || $a)"));
            }
            SECTION("field type") {
                CHECK(row.field_type);
                CHECK_THAT(row.field_type.value(), Equals("VARCHAR"));
            }
            SECTION("nullable") {
                CHECK(row.nullable);
            }
        }
        SECTION("row data#2") {
            auto& row = results[1];

            SECTION("field name") {
                CHECK_THAT(row.field_name, Equals("b"));
            }
            SECTION("field type") {
                CHECK(row.field_type);
                CHECK_THAT(row.field_type.value(), Equals("INTEGER"));
            }
            SECTION("nullable") {
                CHECK(row.nullable);
            }
        }
    }
}
// untyped

#endif
