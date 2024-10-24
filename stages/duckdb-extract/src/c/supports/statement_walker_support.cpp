#include <duckdb.hpp>
#include <duckdb/parser/query_node/list.hpp>
#include <duckdb/parser/tableref/list.hpp>
#include <duckdb/parser/expression/list.hpp>
#include <duckdb/common/extra_type_info.hpp>

#define MAGIC_ENUM_RANGE_MAX (std::numeric_limits<uint8_t>::max())

#include <magic_enum/magic_enum.hpp>
#include "duckdb_params_collector.hpp"
#include  "statement_walker_support.hpp"

namespace worker {

static auto keepColumnName(duckdb::unique_ptr<duckdb::ParsedExpression>& expr) -> std::string {
    if (expr->alias != "") {
        return expr->alias;
    }
    else {
        return expr->ToString();
    }
}

static auto walkOrderBysNodeInternal(ParameterCollector& collector, duckdb::OrderModifier& order_bys, uint32_t depth) -> void;

static auto pickUserTypeName(const duckdb::CastExpression& expr) -> std::optional<std::string> {
    auto *ext_info = expr.cast_type.AuxInfo();
    if (ext_info && (ext_info->type == duckdb::ExtraTypeInfoType::USER_TYPE_INFO)) {
        auto& user_info = ext_info->Cast<duckdb::UserTypeInfo>();
        
        return std::make_optional(user_info.user_type_name);
    }

    return std::nullopt;
}

auto walkExpression(ParameterCollector& collector, duckdb::unique_ptr<duckdb::ParsedExpression>& expr, uint32_t depth) -> void {
    switch (expr->expression_class) {
    case duckdb::ExpressionClass::PARAMETER:
        {
            auto& param_expr = expr->Cast<duckdb::ParameterExpression>();
            param_expr.identifier = collector.ofPosition(param_expr.identifier);
        }
        break;
    case duckdb::ExpressionClass::CAST: 
        {
            auto& cast_expr = expr->Cast<duckdb::CastExpression>();
            auto sv_child = cast_expr.child->Copy();

            walkExpression(collector, cast_expr.child, depth+1);

            if (cast_expr.child->expression_class == duckdb::ExpressionClass::PARAMETER) {
                collector.attachTypeHint(sv_child->Cast<duckdb::ParameterExpression>().identifier, cast_expr.Copy());
            }
        }
        break;
    case duckdb::ExpressionClass::COMPARISON:
        {
            auto& cmp_expr = expr->Cast<duckdb::ComparisonExpression>();
            walkExpression(collector, cmp_expr.left, depth+1);
            walkExpression(collector, cmp_expr.right, depth+1);
        }
        break;
    case duckdb::ExpressionClass::BETWEEN:
        {
            auto& between_expr = expr->Cast<duckdb::BetweenExpression>();
            walkExpression(collector, between_expr.input, depth+1);
            walkExpression(collector, between_expr.lower, depth+1);
            walkExpression(collector, between_expr.upper, depth+1);
        }
        break;
    case duckdb::ExpressionClass::CASE:
        {
            auto& case_expr = expr->Cast<duckdb::CaseExpression>();

            // whrn-then clause
            for (auto& case_check: case_expr.case_checks) {
                walkExpression(collector, case_check.when_expr, depth+1);
                walkExpression(collector, case_check.then_expr, depth+1);
            }
            
            // else clause
            walkExpression(collector, case_expr.else_expr, depth+1);
        }
        break;
    case duckdb::ExpressionClass::CONJUNCTION:
        {
            auto& conj_expr = expr->Cast<duckdb::ConjunctionExpression>();
            
            for (auto& child: conj_expr.children) {
                walkExpression(collector, child, depth+1);
            }
        }
        break;
    case duckdb::ExpressionClass::FUNCTION:
        {
            auto& fn_expr = expr->Cast<duckdb::FunctionExpression>();

            for (auto& child: fn_expr.children) {
                walkExpression(collector, child, depth+1);
            }

            // filter clause
            if (fn_expr.filter) {
                walkExpression(collector, fn_expr.filter, depth+1);
            }
            // order by(s)
            walkOrderBysNodeInternal(collector, *fn_expr.order_bys, depth+1);
        }
        break;
    case duckdb::ExpressionClass::SUBQUERY:
        {
            auto& sq_expr = expr->Cast<duckdb::SubqueryExpression>();

            // left (if any)
            if (sq_expr.subquery_type == duckdb::SubqueryType::ANY) {
                walkExpression(collector, sq_expr.child, depth+1);
            }
            // right
            walkSelectStatementInternal(collector, *sq_expr.subquery, depth+1);
        }
        break;
    case duckdb::ExpressionClass::WINDOW:
        {
            auto& fn_expr = expr->Cast<duckdb::WindowExpression>();

            // function args
            for (auto& child: fn_expr.children) {
                walkExpression(collector, child, depth+1);
            }
        
            for (auto& child: fn_expr.partitions) {
                walkExpression(collector, child, depth+1);
            }
            for (auto& order_by: fn_expr.orders) {
                walkExpression(collector, order_by.expression, depth+1);                
            }
            if (fn_expr.filter_expr) {
                walkExpression(collector, fn_expr.filter_expr, depth+1);
            }
            if (fn_expr.start_expr) {
                walkExpression(collector, fn_expr.start_expr, depth+1);                
            }
            if (fn_expr.end_expr) {
                walkExpression(collector, fn_expr.end_expr, depth+1);                
            }
            if (fn_expr.offset_expr) {
                walkExpression(collector, fn_expr.offset_expr, depth+1);                
            }
            if (fn_expr.default_expr) {
                walkExpression(collector, fn_expr.default_expr, depth+1);                
            }
        }
        break;
    case duckdb::ExpressionClass::CONSTANT:
    case duckdb::ExpressionClass::COLUMN_REF:
        // no conversion
        break;
    case duckdb::ExpressionClass::OPERATOR:
        {
            auto& op_expr = expr->Cast<duckdb::OperatorExpression>();

            for (auto& child: op_expr.children) {
                walkExpression(collector, child, depth+1);
            }
        }
        break;
    case duckdb::ExpressionClass::STAR:
        {
            auto& star_expr = expr->Cast<duckdb::StarExpression>();

            if (star_expr.expr) {
                // handle COLUMNS
                walkExpression(collector, star_expr.expr, depth+1);
            }
            for (auto& rep_expr: star_expr.replace_list | std::views::values) {
                // handle replace list
                walkExpression(collector, rep_expr, depth+1);
            }
        }
        break;
    default: 
        collector.channel.warn(std::format("[TODO] Unsupported expression class: {} (depth: {})", magic_enum::enum_name(expr->expression_class), depth));
        break;
    }
}

static auto walkOrderBysNodeInternal(ParameterCollector& collector, duckdb::OrderModifier& order_bys, uint32_t depth) -> void {
    for (auto& order_by: order_bys.orders) {
        walkExpression(collector, order_by.expression, depth);
    }
}

static auto walkSelectListItem(ParameterCollector& collector, duckdb::unique_ptr<duckdb::ParsedExpression>& expr, uint32_t depth) -> void {
    if (expr->HasParameter() || expr->HasSubquery() || (expr->expression_class == duckdb::ExpressionClass::STAR)) {
        if (depth > 0) {
            walkExpression(collector, expr, depth);
        }
        else {
            auto new_alias = keepColumnName(expr);

            walkExpression(collector, expr, depth);

            if (expr->ToString() != new_alias) {
                expr->alias = new_alias;
            }
        }
    }
}

auto walkReturningList(ParameterCollector& collector, std::vector<duckdb::unique_ptr<duckdb::ParsedExpression>>& expressions) -> void {
    for (auto& expr: expressions) {
        if ((expr->alias == "") && (expr->expression_class != duckdb::ExpressionClass::COLUMN_REF)) {
            expr->alias = expr->ToString();
        }
    }
}

static auto walkTableFunctionArg(ParameterCollector& collector, duckdb::unique_ptr<duckdb::ParsedExpression>&& arg, const std::string& param_name, uint32_t depth) -> duckdb::unique_ptr<duckdb::ParsedExpression> {
    switch (arg->expression_class) {
    case duckdb::ExpressionClass::CONSTANT:
        {
            auto new_arg = duckdb::unique_ptr<duckdb::ParsedExpression>(std::invoke([&param_name]() {
                auto x = duckdb::make_uniq<duckdb::ParameterExpression>();
                x->identifier = param_name;
                return x.release();
            }));

            walkExpression(collector, new_arg, depth);

            auto& const_arg = arg->Cast<duckdb::ConstantExpression>();
            collector.putSampleValue(new_arg->Cast<duckdb::ParameterExpression>().identifier, ExampleKind::General, const_arg.value);

            return new_arg;
        }
    case duckdb::ExpressionClass::CAST:
        {
            auto& cast_arg = arg->Cast<duckdb::CastExpression>();
            cast_arg.child = walkTableFunctionArg(collector, std::move(cast_arg.child), param_name, depth);
            cast_arg.alias = "";

            collector.attachTypeHint(param_name, cast_arg.Copy());

            return arg;
        }
    case duckdb::ExpressionClass::COMPARISON:
        {
            collector.channel.warn(std::format("TODO: Not implemented: {}", magic_enum::enum_name(arg->expression_class)));
            break;
        }
    default:
        collector.channel.warn(std::format("Unsupported table function arg: {}", magic_enum::enum_name(arg->expression_class)));
    }

    return std::move(arg);
}

static auto walkTableFunction(ParameterCollector& collector, duckdb::TableFunctionRef& table_fn, uint32_t depth) -> void {
    if (table_fn.function->expression_class == duckdb::ExpressionClass::FUNCTION) {
        auto& fn_expr = table_fn.function->Cast<duckdb::FunctionExpression>();

        for (auto& arg: fn_expr.children) {
            if (arg->alias.empty() || (! arg->alias.starts_with('$'))) {
                walkExpression(collector, arg, depth+1);
            }
            else {    
                arg = walkTableFunctionArg(collector, std::move(arg), arg->alias.substr(1), depth);
            }
        }
    }
}

auto walkTableRef(ParameterCollector& collector, duckdb::unique_ptr<duckdb::TableRef>& table_ref, uint32_t depth) -> void {
    switch (table_ref->type) {
    case duckdb::TableReferenceType::BASE_TABLE:
    case duckdb::TableReferenceType::EMPTY_FROM:
        // no conversion
        break;
    case duckdb::TableReferenceType::TABLE_FUNCTION:
        {
            auto& table_fn = table_ref->Cast<duckdb::TableFunctionRef>();
            walkTableFunction(collector, table_fn, depth);
        }
        break;
    case duckdb::TableReferenceType::JOIN:
        {
            auto& join_ref = table_ref->Cast<duckdb::JoinRef>();
            walkTableRef(collector, join_ref.left, depth+1);
            walkTableRef(collector, join_ref.right, depth+1);

            if (join_ref.condition) { // positional join has NULL pointer
                walkExpression(collector, join_ref.condition, depth+1);
            }
        }
        break;
    case duckdb::TableReferenceType::SUBQUERY:
        {
            auto& sq_ref = table_ref->Cast<duckdb::SubqueryRef>();
            walkSelectStatementInternal(collector, *sq_ref.subquery, 0);
        }
        break;
    case duckdb::TableReferenceType::EXPRESSION_LIST:
        {
            auto& values_ref = table_ref->Cast<duckdb::ExpressionListRef>();

            for (auto& expr: values_ref.values | std::views::join) {
                walkExpression(collector, expr, depth+1);
            }
        }
        break;
    default:
        collector.channel.warn(std::format("[TODO] Unsupported table ref type: {} (depth: {})", magic_enum::enum_name(table_ref->type), depth));
        break;
    }
}

static auto walkStatementResultModifires(ParameterCollector& collector, duckdb::vector<duckdb::unique_ptr<duckdb::ResultModifier>>& modifiers, uint32_t depth) -> void {
    for (auto& modifier: modifiers) {
        switch (modifier->type) {
        case duckdb::ResultModifierType::LIMIT_MODIFIER:
            {
                auto& mod_lim = modifier->Cast<duckdb::LimitModifier>();
                if (mod_lim.offset) {
                    walkExpression(collector, mod_lim.offset, depth);
                }
                if (mod_lim.limit) {
                    walkExpression(collector, mod_lim.limit, depth);
                }
            }
            break;
        case duckdb::ResultModifierType::ORDER_MODIFIER:
            walkOrderBysNodeInternal(collector, modifier->Cast<duckdb::OrderModifier>(), depth);
            break;
        case duckdb::ResultModifierType::DISTINCT_MODIFIER: 
            break;
        case duckdb::ResultModifierType::LIMIT_PERCENT_MODIFIER:
            {
                auto& mod_lim = modifier->Cast<duckdb::LimitPercentModifier>();
                if (mod_lim.offset) {
                    walkExpression(collector, mod_lim.offset, depth);
                }
                if (mod_lim.limit) {
                    walkExpression(collector, mod_lim.limit, depth);
                }
            }
            break;
        default:
            collector.channel.warn(std::format("[TODO] Not implemented result modifier: {} (depth: {})", magic_enum::enum_name(modifier->type), depth));
            break;
        }
    }
}

static auto walkQueryNode(ParameterCollector& collector, duckdb::unique_ptr<duckdb::QueryNode>& node, uint32_t depth) -> void {
    switch (node->type) {
    case duckdb::QueryNodeType::SELECT_NODE: 
        {
            auto& select_node =  node->Cast<duckdb::SelectNode>();

            cte: {
                walkCTEStatement(collector, node->cte_map);
            }
            select_list: {
                for (auto& expr: select_node.select_list) {
                    walkSelectListItem(collector, expr, depth);
                }
            }
            form_clause: {
                walkTableRef(collector, select_node.from_table, depth+1);
            }
            if (select_node.where_clause) {
                walkExpression(collector, select_node.where_clause, depth+1);
            }
            if (select_node.groups.group_expressions.size() > 0) {
                for (auto& expr: select_node.groups.group_expressions) {
                    walkExpression(collector, expr, depth+1);
                }
            }
            if (select_node.having) {
                walkExpression(collector, select_node.having, depth+1);
            }
            if (select_node.sample) {
                // Sampleclause is not accept placeholder
            }
            if (select_node.qualify) {
                walkExpression(collector, select_node.qualify, depth+1);
            }
            
            walkStatementResultModifires(collector, select_node.modifiers, depth);
        }
        break;
    case duckdb::QueryNodeType::SET_OPERATION_NODE:
        {
            auto& combin_node = node->Cast<duckdb::SetOperationNode>();
            walkQueryNode(collector, combin_node.left, 0);
            walkQueryNode(collector, combin_node.right, 0);
        }
        break;
    case duckdb::QueryNodeType::CTE_NODE: 
        {
            auto& cte_node = node->Cast<duckdb::CTENode>();
            walkQueryNode(collector, cte_node.query, 0);
            walkQueryNode(collector, cte_node.child, 0);
        }
        break;
    case duckdb::QueryNodeType::RECURSIVE_CTE_NODE:
        {
            auto& cte_node = node->Cast<duckdb::RecursiveCTENode>();
            walkQueryNode(collector, cte_node.left, 0);
            walkQueryNode(collector, cte_node.right, 0);
        }
        break;
    default: 
        collector.channel.warn(std::format("[TODO] Unsupported select node: {} (depth: {})", magic_enum::enum_name(node->type), depth));
        break;
    }
}

auto walkSelectStatementInternal(ParameterCollector& collector, duckdb::SelectStatement& stmt, uint32_t depth) -> void {
    walkQueryNode(collector, stmt.node, depth);
}

auto walkCTEStatement(ParameterCollector& collector, duckdb::CommonTableExpressionMap& cte) -> void {
    for (auto& [key, value]: cte.map) {
        walkSelectStatementInternal(collector, *(value->query), 0);
    }
}

}
