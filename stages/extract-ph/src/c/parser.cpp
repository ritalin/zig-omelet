#include <iostream>
#include <charconv>

#include <duckdb/parser/parser.hpp>
#include <duckdb/parser/query_node/select_node.hpp>

#include <duckdb/parser/tableref/table_function_ref.hpp>

#include <duckdb/parser/expression/parameter_expression.hpp>
#include <duckdb/parser/expression/cast_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/conjunction_expression.hpp>
#include <duckdb/parser/expression/comparison_expression.hpp>

#define MAGIC_ENUM_RANGE_MAX (std::numeric_limits<uint8_t>::max())

#include <zmq.h>
#include <magic_enum/magic_enum_iostream.hpp>
#include <nlohmann/json.hpp>

class PlaceholderCollector {
public: 
    struct Entry {
        uint32_t index;
        std::string name;
        std::optional<std::string> type_name;
    };
private:
    void* socket;
    uint32_t max_index = 0;
    std::unordered_map<std::string, Entry> lookup = {};
public:
    PlaceholderCollector(void *socket);
    auto walk(duckdb::unique_ptr<duckdb::SQLStatement>& stmt) -> void;
    auto push_param(std::string param_name) -> std::pair<Entry, bool>;
    auto with_type_hint(std::string param_key, duckdb::LogicalType ty) -> void;
    auto finish(duckdb::unique_ptr<duckdb::SQLStatement>& stmt) -> void;
};

using magic_enum::iostream_operators::operator<<;

// -------------------------

auto walkExpression(PlaceholderCollector *collector, duckdb::unique_ptr<duckdb::ParsedExpression>& expr) -> std::optional<std::string> {
    if (expr->HasParameter()) {
        switch (expr->expression_class) {
        case duckdb::ExpressionClass::PARAMETER: 
            {
                auto& param_expr = expr->Cast<duckdb::ParameterExpression>();
                auto param_name = std::string(param_expr.identifier);
                auto push_result = collector->push_param(param_name);
                param_expr.identifier = std::to_string(push_result.first.index);

                return push_result.second ? std::make_optional(push_result.first.name) : std::nullopt;
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
        default:
            std::cout << "[Todo] unsupported expr: " << expr->type << std::endl;
            break;
        }
    }

    return std::nullopt;
}

auto walkSelectStatementNode(PlaceholderCollector *collector, duckdb::SelectNode& node) -> void {
    for (auto& sel: node.select_list) {
        walkExpression(collector, sel);
    }

    switch (node.from_table->type) {
    case duckdb::TableReferenceType::TABLE_FUNCTION:
        {
            auto& fn_ref = node.from_table->Cast<duckdb::TableFunctionRef>();
            walkExpression(collector, fn_ref.function);
        }
        break;
    case duckdb::TableReferenceType::BASE_TABLE:
        // empty 
        break;
    default:
        std::cout << "[Todo] unsupported table ref: " << node.from_table->type << std::endl;
        break;
    }

    walkExpression(collector, node.where_clause);
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
            { "index", entry.index },
            { "name", entry.name },
        };

        if (entry.type_name) {
            j["type-name"] = entry.type_name.value();
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

auto serializePlaceHolder(std::unordered_map<std::string, PlaceholderCollector::Entry> entries) -> std::string {
    auto items = nlohmann::json::array();

    for (auto& e: entries) {
        items.push_back(ns::to_json(e.second));
    }
    return nlohmann::to_string(items);
}

// -------------------------

PlaceholderCollector::PlaceholderCollector(void *socket) {
    this->socket = socket;
}

auto PlaceholderCollector::walk(duckdb::unique_ptr<duckdb::SQLStatement> &stmt) -> void {
    // auto topic_before = std::string("query:before");
    // zmq_send(this->socket, topic_before.c_str(), topic_before.length(), ZMQ_SNDMORE);
    // auto query = stmt->ToString();
    // auto len = zmq_send(this->socket, query.c_str(), query.length(), 0);
    // std::cout << "Send:byte: " << len << std::endl;

    // std::cout << std::endl << "[Before]" << std::endl;
    // std::cout << stmt->ToString() << std::endl << std::endl;

    switch (stmt->type) {
    case duckdb::StatementType::SELECT_STATEMENT:
        {
            auto& sel_stmt = stmt->Cast<duckdb::SelectStatement>();
            walkSelectStatement(this, sel_stmt);
            
            this->finish(stmt);

            // std::cout << std::endl << "[After]" << std::endl;
            // std::cout << stmt->ToString() << std::endl << std::endl;
        }
        break;
    default:
        std::cout << "[Todo] unsupported statement: " << stmt->type << std::endl;
        break;
    }
}

auto PlaceholderCollector::push_param(std::string param_name) -> std::pair<Entry, bool> {
    auto it = this->lookup.find(param_name);
    if (it != lookup.end()) {
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
        it->second.type_name = ty.ToString();
    }
}

auto PlaceholderCollector::finish(duckdb::unique_ptr<duckdb::SQLStatement>& stmt) -> void {
    send_query: {
        auto topic = std::string("query");
        zmq_send(this->socket, topic.c_str(), topic.length(), ZMQ_SNDMORE);
        auto query = stmt->ToString();
        zmq_send(this->socket, query.c_str(), query.length(), 0);

        std::cout << "publish: " << topic << " - " << query << std::endl;
    }
    send_place_holder: {
        auto topic = std::string("place-holder");
        zmq_send(this->socket, topic.c_str(), topic.length(), ZMQ_SNDMORE);
        auto ph = serializePlaceHolder(this->lookup);
        zmq_send(this->socket, ph.c_str(), ph.length(), 0);

        std::cout << "publish: " << topic << " - " << ph << std::endl;
    }
}

// -------------------------

extern "C" {

auto duckDbParseSQL(const char *query, uint32_t len, void *socket) -> void {
    auto parser = duckdb::Parser();
    parser.ParseQuery(std::string(query, len));

    PlaceholderCollector collector(socket);

    for (auto& stmt: parser.statements) {
        collector.walk(stmt);
    }
}

}