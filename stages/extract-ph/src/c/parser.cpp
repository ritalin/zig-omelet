#include <iostream>
#include <charconv>

#include <duckdb/parser/parser.hpp>
#include <duckdb/parser/query_node/select_node.hpp>

#include <duckdb/parser/tableref/list.hpp>
#include <duckdb/parser/expression/list.hpp>
#include <duckdb/common/exception/parser_exception.hpp>

#define MAGIC_ENUM_RANGE_MAX (std::numeric_limits<uint8_t>::max())

#include <zmq.h>
#include <magic_enum/magic_enum_iostream.hpp>
#include <nlohmann/json.hpp>

#include "cbor_encode.hpp"
#include "parse_duckdb.h"

class PlaceholderCollector {
public: 
    struct Entry {
        uint32_t index;
        std::string field_name;
        std::optional<std::string> field_type;
    };
    struct WalkResult {
        typedef std::pair<std::string, Entry> Placeholder;

        std::string id;
        std::string query;
        uint32_t max_index;
        std::unordered_map<std::string, Entry> lookup;
    public:
        auto placeholders() const -> std::vector<Placeholder>;
    };
private:
    std::string id;
    std::optional<void*> socket;
    uint32_t max_index = 0;
    std::unordered_map<std::string, Entry> lookup;
public:
    PlaceholderCollector(std::string id): PlaceholderCollector(id, nullptr) {}
    PlaceholderCollector(std::string id, void* socket): id(std::move(id)), socket(std::make_optional(socket)) {}
public:
    auto walk(duckdb::unique_ptr<duckdb::SQLStatement>& stmt) -> WalkResult;
    auto push_param(std::string param_name) -> std::pair<Entry, bool>;
    auto with_type_hint(std::string param_key, duckdb::LogicalType ty) -> void;
public:
    auto finish(const WalkResult& result) -> void;
    auto err(const std::string msg) -> void;
};

using magic_enum::iostream_operators::operator<<;

// -------------------------

auto walkSelectStatement(PlaceholderCollector *collector, duckdb::SelectStatement& stmt) -> void;

auto walkExpression(PlaceholderCollector *collector, duckdb::unique_ptr<duckdb::ParsedExpression>& expr) -> std::optional<std::string> {
    if (expr->HasParameter() || expr->HasSubquery()) {
        switch (expr->expression_class) {
        case duckdb::ExpressionClass::PARAMETER:
            {
                auto& param_expr = expr->Cast<duckdb::ParameterExpression>();
                auto param_name = std::string(param_expr.identifier);
                auto push_result = collector->push_param(param_name);
                param_expr.identifier = std::to_string(push_result.first.index);

                return push_result.second ? std::make_optional(push_result.first.field_name) : std::nullopt;
            }
        case duckdb::ExpressionClass::CAST: 
            {
                auto& cast_expr = expr->Cast<duckdb::CastExpression>();
                if (cast_expr.child->expression_class == duckdb::ExpressionClass::PARAMETER) {
                    auto param_key = walkExpression(collector, cast_expr.child);
                    expr.swap(cast_expr.child);
                    cast_expr.child.release();

                    if (param_key) {
                        collector->with_type_hint(param_key.value(), cast_expr.cast_type);
                    }
                }
            }
            break;
        case duckdb::ExpressionClass::FUNCTION:
            {
                auto& fn_expr = expr->Cast<duckdb::FunctionExpression>();
                for (auto& arg: fn_expr.children) {
                    walkExpression(collector, arg);
                }
            }
            break;
        case duckdb::ExpressionClass::CONJUNCTION:
            {
                auto& conj_expr = expr->Cast<duckdb::ConjunctionExpression>();
                for (auto& child: conj_expr.children) {
                    walkExpression(collector, child);
                }
            }
            break;
        case duckdb::ExpressionClass::COMPARISON:
            {
                auto& cmp_expr = expr->Cast<duckdb::ComparisonExpression>();
                walkExpression(collector, cmp_expr.left);
                walkExpression(collector, cmp_expr.right);
            }
            break;
        case duckdb::ExpressionClass::COLUMN_REF:
            // empty
            break;
        case duckdb::ExpressionClass::OPERATOR:
            {
                auto& op_expr = expr->Cast<duckdb::OperatorExpression>();
                for (auto& child: op_expr.children) {
                    walkExpression(collector, child);
                }
            }
            break;
        case duckdb::ExpressionClass::BETWEEN:
            {
                auto& cmp_expr = expr->Cast<duckdb::BetweenExpression>();
                walkExpression(collector, cmp_expr.input);
                walkExpression(collector, cmp_expr.lower);
                walkExpression(collector, cmp_expr.upper);
            }
            break;
        case duckdb::ExpressionClass::CASE:
            {
                auto& case_expr = expr->Cast<duckdb::CaseExpression>();
                for (auto& c: case_expr.case_checks) {
                    walkExpression(collector, c.when_expr);
                    walkExpression(collector, c.then_expr);
                }
                walkExpression(collector, case_expr.else_expr);
            }
            break;
        case duckdb::ExpressionClass::SUBQUERY:
            {
                auto& sq_expr = expr->Cast<duckdb::SubqueryExpression>();
                walkSelectStatement(collector, *sq_expr.subquery);

                if (sq_expr.subquery_type == duckdb::SubqueryType::ANY) {
                    walkExpression(collector, sq_expr.child);
                }
            }
            break;
        default:
            std::cout << "[Todo] unsupported expr: " << expr->type << "(class: " << expr->expression_class << ")" << std::endl;
            break;
        }
    }

    return std::nullopt;
}

auto walkTableRef(PlaceholderCollector *collector, duckdb::unique_ptr<duckdb::TableRef>& table_ref) -> void {
    switch (table_ref->type) {
    case duckdb::TableReferenceType::TABLE_FUNCTION:
        {
            auto& fn_ref = table_ref->Cast<duckdb::TableFunctionRef>();
            walkExpression(collector, fn_ref.function);
        }
        break;
    case duckdb::TableReferenceType::JOIN:
        {
            auto& join_ref = table_ref->Cast<duckdb::JoinRef>();
            walkTableRef(collector, join_ref.left);
            walkTableRef(collector, join_ref.right);
            walkExpression(collector, join_ref.condition);
        }
        break;
    case duckdb::TableReferenceType::SUBQUERY:
        {
            auto& sq_ref = table_ref->Cast<duckdb::SubqueryRef>();
            walkSelectStatement(collector, *sq_ref.subquery);
        }
        break;
    case duckdb::TableReferenceType::BASE_TABLE:
    case duckdb::TableReferenceType::EMPTY_FROM:
        // empty 
        break;
    default:
        std::cout << "[Todo] unsupported table ref: " << table_ref->type << std::endl;
        break;
    }
}

auto walkSelectStatementNode(PlaceholderCollector *collector, duckdb::SelectNode& node) -> void {
    for (auto& sel: node.select_list) {
        walkExpression(collector, sel);
    }

    walkTableRef(collector, node.from_table);

    if (node.where_clause) {
        walkExpression(collector, node.where_clause);
    }
}

auto walkSelectStatement(PlaceholderCollector *collector, duckdb::SelectStatement& stmt) -> void {
    auto& node = stmt.node;

    switch (node->type) {
    case duckdb::QueryNodeType::SELECT_NODE:
        walkSelectStatementNode(collector, node->Cast<duckdb::SelectNode>());
        break;
    default:
        std::cout << "[Todo] unsupported select stmt node: " << node->type << std::endl;
        break;
    }
}

namespace ns {
    auto to_json(const PlaceholderCollector::Entry& entry) -> nlohmann::json {
        auto j = nlohmann::json {
            // { "index", entry.index },
            { "field_name", entry.field_name },
        };

        if (entry.field_type) {
            j["field_type"] = entry.field_type.value();
        }

        return j;
    }

    // auto from_json(const nlohmann::json& j, PlaceholderCollector::Entry& entry) -> void {
    //     j.at("index").get_to(entry.index);
    //     j.at("name").get_to(entry.name);

    //     if (j.contains("type-name")) {
    //         std::string ty;
    //         j.at("type-name").get_to(ty);
    //         entry.type_name = ty;
    //     }
    // }
}

auto serializePlaceHolder(const std::vector<PlaceholderCollector::WalkResult::Placeholder>& entries) -> std::string {
    auto items = nlohmann::json::array();

    for (auto& e: entries) {
        items.push_back(ns::to_json(e.second));
    }
    return nlohmann::to_string(items);
}

// -------------------------

auto PlaceholderCollector::WalkResult::placeholders() const -> std::vector<Placeholder> {
    auto entries = std::vector<std::pair<std::string, Entry>>(this->lookup.begin(), this->lookup.end());
    std::sort(entries.begin(), entries.end(), [](WalkResult::Placeholder lhs, WalkResult::Placeholder rhs) {
        return std::greater<uint32_t>()(lhs.second.index, rhs.second.index);
    });

    return std::move(entries);
}

auto PlaceholderCollector::walk(duckdb::unique_ptr<duckdb::SQLStatement> &stmt) -> WalkResult {
    // auto topic_before = std::string("query:before");
    // zmq_send(this->socket, topic_before.c_str(), topic_before.length(), ZMQ_SNDMORE);
    // auto query = stmt->ToString();
    // auto len = zmq_send(this->socket, query.c_str(), query.length(), 0);
    // std::cout << "Send:byte: " << len << std::endl;

    // std::cout << std::endl << "[Before]" << std::endl;
    // std::cout << stmt->ToString() << std::endl << std::endl;

    this->max_index = 0;
    this->lookup = {};

    switch (stmt->type) {
    case duckdb::StatementType::SELECT_STATEMENT:
        {
            auto& sel_stmt = stmt->Cast<duckdb::SelectStatement>();
            walkSelectStatement(this, sel_stmt);
            
            // std::cout << std::endl << "[After]" << std::endl;
            // std::cout << stmt->ToString() << std::endl << std::endl;
        }
        break;
    default:
        std::cout << "[Todo] unsupported statement: " << stmt->type << std::endl;
        break;
    }

    return WalkResult { this->id, stmt->ToString(), this->max_index, this->lookup };
}

auto PlaceholderCollector::push_param(std::string param_name) -> std::pair<Entry, bool> {
    auto it = this->lookup.find(param_name);
    if (it != this->lookup.end()) {
        //  found
        return std::make_pair(it->second, false);
    }

    uint32_t value {};
    auto positional = std::from_chars(std::__to_address(param_name.cbegin()), std::__to_address(param_name.cend()), value);
    if (positional.ec == std::errc{}) {
        this->max_index = std::max(this->max_index, value);
        std::string k = std::to_string(value);
        auto e = Entry { value, param_name, std::nullopt };
        this->lookup.emplace(k, e);

        return std::make_pair(e, true);
    }
    else {
        auto new_position = ++this->max_index;
        auto e = Entry { new_position, param_name, std::nullopt };
        this->lookup.emplace(param_name, e);

        return std::make_pair(e, true);
    }
}

auto PlaceholderCollector::with_type_hint(std::string param_key, duckdb::LogicalType ty) -> void {
    auto it = this->lookup.find(param_key);
    if (it != this->lookup.end()) {
        it->second.field_type = ty.ToString();
    }
}

auto PlaceholderCollector::finish(const WalkResult& result) -> void {
    if (! this->socket) return;

    auto socket = this->socket.value();
    event_type: {
        auto event_type = std::string("worker_result");
        zmq_send(socket, event_type.data(), event_type.length(), ZMQ_SNDMORE);    
    }
    payload: {
        std::vector<char> buf;

        const size_t STMT_OFFSET = 1;
        const size_t STMT_COUNT = 1;

        CborEncoder payload_encoder;
        result_tag: {
            payload_encoder.addString("topic_body");
        }
        source_path: {
            payload_encoder.addString(result.id);
        }
        stmt_count: {
            payload_encoder.addUInt(STMT_COUNT);
        }
        stmt_offset: {
            payload_encoder.addUInt(STMT_OFFSET);
        }
        topic_body: {
            CborEncoder encoder;
            encoder.addArrayHeader(2);
            send_query: {
                encoder.addStringPair(TOPIC_QUERY, result.query);
            }
            send_placeholder: {
                auto ph = serializePlaceHolder(result.placeholders());
                encoder.addStringPair(TOPIC_PH, ph);
            }

            payload_encoder.concatBinary(encoder);
        }

        auto encode_result = payload_encoder.build();

        zmq_send(socket, encode_result.data(), encode_result.length(), ZMQ_SNDMORE);
        zmq_send(socket, "", 0, 0);    
    }
}

auto PlaceholderCollector::err(const std::string msg) -> void {
    if (! this->socket) {
        std::cout << "[ERROR] " << msg << std::endl;
    }
    else {
        auto socket = this->socket.value();

        event_type: {
            auto event_type = std::string("worker_result");
            zmq_send(socket, event_type.data(), event_type.length(), ZMQ_SNDMORE);    
        }
        payload: {
            CborEncoder payload_encoder;

            event_tag: {
                payload_encoder.addString("log");
            }
            source_path: {
                payload_encoder.addString(this->id);
            }
            log_level: {
                payload_encoder.addString("err");
            }
            log_content: {
                payload_encoder.addString(msg);
            }

            auto encode_result = payload_encoder.build();
            zmq_send(socket, encode_result.c_str(), encode_result.length(), ZMQ_SNDMORE);    
            zmq_send(socket, "", 0, 0);    
        }
    }
}


auto operator==(const PlaceholderCollector::Entry& lhs, const PlaceholderCollector::Entry& rhs) -> bool {
    if (lhs.index != rhs.index) return false;
    if (lhs.field_name != rhs.field_name) return false;
    if (lhs.field_type != rhs.field_type) return false;
    
    return true;
}

// -------------------------

extern "C" {

auto initCollector(const char *id, size_t id_len, void *socket, CollectorRef *handle) -> int32_t {
    *handle = reinterpret_cast<CollectorRef>(new PlaceholderCollector(std::string(id, id_len), socket));

    return 0;
}

auto deinitCollector(CollectorRef handle) -> void {
    delete reinterpret_cast<PlaceholderCollector *>(handle);
}

auto parseDuckDbSQL(CollectorRef handle, const char *query, size_t query_len) -> void {
    auto collector = reinterpret_cast<PlaceholderCollector *>(handle);
    
    try {
        auto parser = duckdb::Parser();
        parser.ParseQuery(std::string(query, query_len));

        if (parser.statements.size() > 0) {
            auto result = collector->walk(parser.statements[0]);

            collector->finish(result);            
        }
    }
    catch (const duckdb::ParserException& ex) {
        collector->err(ex.what());
    }
}

auto duckDbParseSQL(const char *id, const char *query, uint32_t len, void *socket) -> void {
    try {
        auto parser = duckdb::Parser();
        parser.ParseQuery(std::string(query, len));

        if (parser.statements.size() > 0) {
            PlaceholderCollector collector(std::string(id), socket);
            auto result = collector.walk(parser.statements[0]);

            collector.finish(result);            
        }
    }
    catch (const duckdb::ParserException& ex) {
        std::cout << "[ERROR] " << ex.what() << std::endl;
        PlaceholderCollector collector(std::string(id), socket);
        collector.err(ex.what());
    }
}

}

#ifndef DISABLE_CATCH2_TEST

// -------------------------
// Unit tests
// -------------------------

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

using namespace Catch::Matchers;

TEST_CASE("Symtax error select statement") {
    std::string query("SELCT a, b, c");

    // duckdb::ParseException thrown
    REQUIRE_THROWS_AS(
        [&]{
            auto parser = duckdb::Parser();
            parser.ParseQuery(std::string(query.c_str(), query.size()));
        }(),
        duckdb::ParserException
    );
}

TEST_CASE("Fromless select statement") {
    std::string query("select a, b, c");
    const std::string expect_query("SELECT a, b, c");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");

    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 0);
    }
    SECTION("picked up placeholder info") {
        CHECK(result.lookup.size() == 0);
    }
}

TEST_CASE("No placeholder select statement") {
    std::string query("select a, b, c, from foo");
    const std::string expect_query("SELECT a, b, c FROM foo");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");

    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 0);
    }
    SECTION("picked up placeholder info") {
        CHECK(result.lookup.size() == 0);
    }
}

TEST_CASE("No placeholder select statement with where clause") {
    std::string query("select a, b, c, from foo where x = 0");
    const std::string expect_query("SELECT a, b, c FROM foo WHERE (x = 0)");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");
    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 0);
    }
    SECTION("picked up placeholder info") {
        CHECK(result.lookup.size() == 0);
    }
}

TEST_CASE("Select clause typed positional placeholder in select statement") {
    std::string query("select a, $1::int, c, from foo where x = 0");
    const std::string expect_query("SELECT a, $1, c FROM foo WHERE (x = 0)");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");
    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 1);
    }
    SECTION("picked up placeholder") {
        CHECK(result.lookup.size() == 1);

        SECTION("picked up placeholder info#1") {
            REQUIRE(result.lookup.contains("1"));

            auto& entry = result.lookup["1"];
            CHECK(entry == PlaceholderCollector::Entry{1, "1", std::make_optional(std::string("INTEGER"))});
        }
    }
}

TEST_CASE("Select clause typed named placeholder in select statement") {
    std::string query("select a, $quantity::int, c, from foo where x = 0");
    const std::string expect_query("SELECT a, $1, c FROM foo WHERE (x = 0)");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");
    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 1);
    }
    SECTION("picked up placeholder") {
        CHECK(result.lookup.size() == 1);
        
        SECTION("picked up placeholder info#1") {
            REQUIRE(result.lookup.contains("quantity"));

            auto& entry = result.lookup["quantity"];
            CHECK(entry == PlaceholderCollector::Entry{1, "quantity", std::make_optional(std::string("INTEGER"))});
        }
    }
}

TEST_CASE("Select clause untyped positional placeholder in select statement") {
    std::string query("select a, $1, c, from foo where x = 0");
    const std::string expect_query("SELECT a, $1, c FROM foo WHERE (x = 0)");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");
    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 1);
    }
    SECTION("picked up placeholder") {
        CHECK(result.lookup.size() == 1);
        
        SECTION("picked up placeholder info#1") {
            REQUIRE(result.lookup.contains("1"));

            auto& entry = result.lookup["1"];
            CHECK(entry == PlaceholderCollector::Entry{1, "1", std::nullopt});
        }
    }
}

TEST_CASE("Select clause auto incl positional placeholder in select statement") {
    std::string query("select a, $1, ?::int4, from foo where x = 0");
    const std::string expect_query("SELECT a, $1, $2 FROM foo WHERE (x = 0)");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");
    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 2);
    }

    SECTION("picked up placeholder") {
        CHECK(result.lookup.size() == 2);

        SECTION("picked up placeholder info#1") {
            REQUIRE(result.lookup.contains("1"));

            auto& entry = result.lookup["1"];
            CHECK(entry == PlaceholderCollector::Entry{1, "1", std::nullopt});
        }
        SECTION("picked up placeholder info#2") {
            REQUIRE(result.lookup.contains("2"));

            auto& entry = result.lookup["2"];
            CHECK(entry == PlaceholderCollector::Entry{2, "2", std::make_optional("INTEGER")});
        }
    }
}

TEST_CASE("Where clause typed named placeholder in select statement") {
    std::string query("select a, b, c, from foo where x = $some_value::text");
    const std::string expect_query("SELECT a, b, c FROM foo WHERE (x = $1)");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");
    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 1);
    }
    SECTION("picked up placeholder") {
        CHECK(result.lookup.size() == 1);

        SECTION("picked up placeholder info#1") {
            REQUIRE(result.lookup.contains("some_value"));

            auto& entry = result.lookup["some_value"];
            CHECK(entry == PlaceholderCollector::Entry{1, "some_value", std::make_optional("VARCHAR")});
        }
    }
}

TEST_CASE("Table func named placeholder in select statement") {
    std::string query("select a, b, c, from read_json($path)");
    const std::string expect_query("SELECT a, b, c FROM read_json($1)");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");
    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 1);
    }
    SECTION("picked up placeholder") {
        CHECK(result.lookup.size() == 1);

        SECTION("picked up placeholder info#1") {
            REQUIRE(result.lookup.contains("path"));

            auto& entry = result.lookup["path"];
            CHECK(entry == PlaceholderCollector::Entry{1, "path", std::nullopt});
        }
    }
}

TEST_CASE("IN operator named placeholder in select statement") {
    std::string query = R"(
        select a, b, c, 
        from foo
        where v in ($value_1::int, $value_2::int)
    )";
    const std::string expect_query("SELECT a, b, c FROM foo WHERE (v IN ($1, $2))");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");
    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 2);
    }
    SECTION("picked up placeholder") {
        CHECK(result.lookup.size() == 2);

        SECTION("picked up placeholder info#1") {
            REQUIRE(result.lookup.contains("value_1"));

            auto& entry = result.lookup["value_1"];
            CHECK(entry == PlaceholderCollector::Entry{1, "value_1", std::make_optional("INTEGER")});
        }
        SECTION("picked up placeholder info#2") {
            REQUIRE(result.lookup.contains("value_2"));

            auto& entry = result.lookup["value_2"];
            CHECK(entry == PlaceholderCollector::Entry{2, "value_2", std::make_optional("INTEGER")});
        }
    }
}
TEST_CASE("NOT IN operator named placeholder in select statement") {
    std::string query = R"(
        select a, b, c, 
        from foo
        where v not in ($value_1::int, $value_2::int)
    )";
    const std::string expect_query("SELECT a, b, c FROM foo WHERE (v NOT IN ($1, $2))");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");
    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 2);
    }
    SECTION("picked up placeholder") {
        CHECK(result.lookup.size() == 2);

        SECTION("picked up placeholder info#1") {
            REQUIRE(result.lookup.contains("value_1"));

            auto& entry = result.lookup["value_1"];
            CHECK(entry == PlaceholderCollector::Entry{1, "value_1", std::make_optional("INTEGER")});
        }
        SECTION("picked up placeholder info#2") {
            REQUIRE(result.lookup.contains("value_2"));

            auto& entry = result.lookup["value_2"];
            CHECK(entry == PlaceholderCollector::Entry{2, "value_2", std::make_optional("INTEGER")});
        }
    }
}

TEST_CASE("BETWEEN operator named placeholder in select statement") {
    std::string query = R"(
        select a, b, c, 
        from foo
        where v between $value_a::text and $value_b::text
    )";
    const std::string expect_query("SELECT a, b, c FROM foo WHERE (v BETWEEN $1 AND $2)");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");
    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 2);
    }
    SECTION("picked up placeholder") {
        CHECK(result.lookup.size() == 2);

        SECTION("picked up placeholder info#1") {
            REQUIRE(result.lookup.contains("value_a"));

            auto& entry = result.lookup["value_a"];
            CHECK(entry == PlaceholderCollector::Entry{1, "value_a", std::make_optional("VARCHAR")});
        }
        SECTION("picked up placeholder info#2") {
            REQUIRE(result.lookup.contains("value_b"));

            auto& entry = result.lookup["value_b"];
            CHECK(entry == PlaceholderCollector::Entry{2, "value_b", std::make_optional("VARCHAR")});
        }
    }
}

TEST_CASE("named placeholder at WHERE function param in select statement") {
    std::string query = R"(
        select a, b, c, 
        from foo
        where s = upper($phrase::text)
    )";
    const std::string expect_query("SELECT a, b, c FROM foo WHERE (s = upper($1))");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");
    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 1);
    }
    SECTION("picked up placeholder") {
        CHECK(result.lookup.size() == 1);

        SECTION("picked up placeholder info#1") {
            REQUIRE(result.lookup.contains("phrase"));

            auto& entry = result.lookup["phrase"];
            CHECK(entry == PlaceholderCollector::Entry{1, "phrase", std::make_optional("VARCHAR")});
        }
    }
}

TEST_CASE("case expr named placeholder in select statement") {
    std::string query = R"(
        select a, b, (case when f = 1 and g = 2 and h = 3 then $x1::varchar else $x2::varchar end) as c, 
        from foo
    )";
    const std::string expect_query("SELECT a, b, CASE  WHEN (((f = 1) AND (g = 2) AND (h = 3))) THEN ($1) ELSE $2 END AS c FROM foo");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");
    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 2);
    }
    SECTION("picked up placeholder") {
        CHECK(result.lookup.size() == 2);

        SECTION("picked up placeholder info#1") {
            REQUIRE(result.lookup.contains("x1"));

            auto& entry = result.lookup["x1"];
            CHECK(entry == PlaceholderCollector::Entry{1, "x1", std::make_optional("VARCHAR")});
        }
        SECTION("picked up placeholder info#2") {
            REQUIRE(result.lookup.contains("x2"));

            auto& entry = result.lookup["x2"];
            CHECK(entry == PlaceholderCollector::Entry{2, "x2", std::make_optional("VARCHAR")});
        }
    }
}

TEST_CASE("case expr named placeholder w/o else in select statement") {
    std::string query = R"(
        select a, b, (case when f = 1 then $x1::varchar end) as c, 
        from foo
    )";
    const std::string expect_query("SELECT a, b, CASE  WHEN ((f = 1)) THEN ($1) ELSE NULL END AS c FROM foo");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");
    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 1);
    }
    SECTION("picked up placeholder") {
        CHECK(result.lookup.size() == 1);

        SECTION("picked up placeholder info#1") {
            REQUIRE(result.lookup.contains("x1"));

            auto& entry = result.lookup["x1"];
            CHECK(entry == PlaceholderCollector::Entry{1, "x1", std::make_optional("VARCHAR")});
        }
    }
}

TEST_CASE("nested case expr named placeholder in select statement") {
    std::string query = R"(
        select 
            a, b, 
            (case when f = 1 
                then $x1::varchar 
                else (case when g = 2
                    then $x2::varchar
                end)
            end) as c, 
        from foo
    )";
    const std::string expect_query("SELECT a, b, CASE  WHEN ((f = 1)) THEN ($1) ELSE CASE  WHEN ((g = 2)) THEN ($2) ELSE NULL END END AS c FROM foo");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");
    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 2);
    }
    SECTION("picked up placeholder") {
        CHECK(result.lookup.size() == 2);

        SECTION("picked up placeholder info#1") {
            REQUIRE(result.lookup.contains("x1"));

            auto& entry = result.lookup["x1"];
            CHECK(entry == PlaceholderCollector::Entry{1, "x1", std::make_optional("VARCHAR")});
        }
        SECTION("picked up placeholder info#2") {
            REQUIRE(result.lookup.contains("x2"));

            auto& entry = result.lookup["x2"];
            CHECK(entry == PlaceholderCollector::Entry{2, "x2", std::make_optional("VARCHAR")});
        }
    }
}

TEST_CASE("case expr named placeholder in select statement#2") {
    std::string query = R"(
        select 
            a, b, 
            (case f 
                when 1 then $x1::varchar 
                when 2 then $x2::varchar
                else $x3::varchar
            end) as c, 
        from foo
    )";
    const std::string expect_query("SELECT a, b, CASE  WHEN ((f = 1)) THEN ($1) WHEN ((f = 2)) THEN ($2) ELSE $3 END AS c FROM foo");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");
    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 3);
    }
    SECTION("picked up placeholder") {
        CHECK(result.lookup.size() == 3);

        SECTION("picked up placeholder info#1") {
            REQUIRE(result.lookup.contains("x1"));

            auto& entry = result.lookup["x1"];
            CHECK(entry == PlaceholderCollector::Entry{1, "x1", std::make_optional("VARCHAR")});
        }
        SECTION("picked up placeholder info#2") {
            REQUIRE(result.lookup.contains("x2"));

            auto& entry = result.lookup["x2"];
            CHECK(entry == PlaceholderCollector::Entry{2, "x2", std::make_optional("VARCHAR")});
        }
        SECTION("picked up placeholder info#3") {
            REQUIRE(result.lookup.contains("x3"));

            auto& entry = result.lookup["x3"];
            CHECK(entry == PlaceholderCollector::Entry{3, "x3", std::make_optional("VARCHAR")});
        }
    }
}

TEST_CASE("named placeholder of scalar subquery in select statement") {
    std::string query = R"(
        select  
            (
                select bar.a from bar
                where
                    bar.f = foo.f
                    and bar.g = $v::int
            ) as c,
        from foo
    )";
    const std::string expect_query("SELECT (SELECT bar.a FROM bar WHERE ((bar.f = foo.f) AND (bar.g = $1))) AS c FROM foo");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");
    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 1);
    }
    SECTION("picked up placeholder") {
        CHECK(result.lookup.size() == 1);

        SECTION("picked up placeholder info#1") {
            REQUIRE(result.lookup.contains("v"));

            auto& entry = result.lookup["v"];
            CHECK(entry == PlaceholderCollector::Entry{1, "v", std::make_optional("INTEGER")});
        }
    }
}

TEST_CASE("named placeholder of exists subquery in select statement") {
    std::string query = R"(
        select  
            exists (
                from bar
                where
                    bar.f = foo.f
                    and bar.g = $v::int
            ) as c,
        from foo
    )";
    const std::string expect_query("SELECT EXISTS(SELECT * FROM bar WHERE ((bar.f = foo.f) AND (bar.g = $1))) AS c FROM foo");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");
    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 1);
    }
    SECTION("picked up placeholder") {
        CHECK(result.lookup.size() == 1);

        SECTION("picked up placeholder info#1") {
            REQUIRE(result.lookup.contains("v"));

            auto& entry = result.lookup["v"];
            CHECK(entry == PlaceholderCollector::Entry{1, "v", std::make_optional("INTEGER")});
        }
    }
}

TEST_CASE("named placeholder of subquery condition in select statement") {
   std::string query = R"(
        select foo.*, 
        from foo
        where 
            a = ANY(
            select bar.c
            from bar
            where bar.g = $v::int
        ) 
    )";
    const std::string expect_query("SELECT foo.* FROM foo WHERE (a = ANY(SELECT bar.c FROM bar WHERE (bar.g = $1)))");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");
    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 1);
    }
    SECTION("picked up placeholder") {
        CHECK(result.lookup.size() == 1);
        
    }
}

TEST_CASE("named placeholder of lateral join subquery in select statement") {
    std::string query = R"(
        select foo.*, b.c
        from foo
        join lateral (
            select bar.c
            from bar
            where
                bar.f = foo.f
                and bar.g = $v::int
        ) b on true
    )";
    const std::string expect_query("SELECT foo.*, b.c FROM foo INNER JOIN (SELECT bar.c FROM bar WHERE ((bar.f = foo.f) AND (bar.g = $1))) AS b ON (CAST('t' AS BOOLEAN))");

    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query.c_str(), query.size()));

    CHECK(parser.statements.size() == 1);

    auto& stmt = parser.statements[0];
    PlaceholderCollector collector("");
    auto result = collector.walk(stmt);

    SECTION("walk away query") {
        CHECK_THAT(stmt->ToString(), Equals(expect_query));
    }
    SECTION("potitional placeholder index") {
        CHECK(result.max_index == 1);
    }
    SECTION("picked up placeholder") {
        CHECK(result.lookup.size() == 1);

        SECTION("picked up placeholder info#1") {
            REQUIRE(result.lookup.contains("v"));

            auto& entry = result.lookup["v"];
            CHECK(entry == PlaceholderCollector::Entry{1, "v", std::make_optional("INTEGER")});
        }
    }
}
#endif