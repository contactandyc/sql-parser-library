// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_node.h"
#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/date_utils.h"
#include "a-memory-library/aml_pool.h"
#include <strings.h>

const char *sql_token_type_name(sql_token_type_t type) {
    switch (type) {
        case SQL_NUMBER: return "NUMBER";
        case SQL_OPERATOR: return "OPERATOR";
        case SQL_COMPARISON: return "COMPARISON";
        case SQL_AND: return "AND";
        case SQL_OR: return "OR";
        case SQL_NOT: return "NOT";
        case SQL_OPEN_PAREN: return "OPEN_PAREN";
        case SQL_CLOSE_PAREN: return "CLOSE_PAREN";
        case SQL_OPEN_BRACKET: return "OPEN_BRACKET";
        case SQL_CLOSE_BRACKET: return "CLOSE_BRACKET";
        case SQL_COMMA: return "COMMA";
        case SQL_SEMICOLON: return "SEMICOLON";
        case SQL_KEYWORD: return "KEYWORD";
        case SQL_FUNCTION: return "FUNCTION";
        case SQL_FUNCTION_LITERAL: return "FUNCTION_LITERAL";
        case SQL_COMMENT: return "COMMENT";
        case SQL_IDENTIFIER: return "IDENTIFIER";
        case SQL_COMPOUND_LITERAL: return "COMPOUND_LITERAL";
        case SQL_LITERAL: return "LITERAL";
        case SQL_NULL: return "NULL";
        case SQL_TOKEN: return "TOKEN";
        case SQL_STAR: return "STAR";
        case SQL_LIST: return "LIST";
        default: return "UNKNOWN";
    }
}

const char *sql_data_type_name(sql_data_type_t type) {
    switch (type) {
        case SQL_TYPE_INT: return "INT";
        case SQL_TYPE_STRING: return "STRING";
        case SQL_TYPE_DOUBLE: return "DOUBLE";
        case SQL_TYPE_DATETIME: return "DATETIME";
        case SQL_TYPE_BOOL: return "BOOL";
        case SQL_TYPE_FUNCTION: return "FUNCTION";
        case SQL_TYPE_CUSTOM: return "CUSTOM";
        default: return "UNKNOWN";
    }
}

bool is_literal(sql_node_t *node) {
    sql_token_type_t type = node->token_type;
    return type == SQL_LITERAL ||
           type == SQL_COMPOUND_LITERAL ||
           type == SQL_NULL ||
           type == SQL_NUMBER ||
           type == SQL_LIST;
}

static sql_node_t *create_convert_node(sql_ctx_t *context, sql_node_t *param, sql_data_type_t target_type) {
    sql_node_t *node = sql_function_init(context, "CONVERT");
    node->data_type = target_type;
    node->num_parameters = 2;
    node->parameters = (sql_node_t **)aml_pool_alloc(context->pool, 2 * sizeof(sql_node_t *));
    node->parameters[1] = param;
    node->parameters[0] = sql_string_init(context, sql_data_type_name(target_type), false);
    node->spec = sql_ctx_get_spec(context, "CONVERT");

    if(node->spec) {
        sql_ctx_spec_update_t *update = node->spec->update(context, node->spec, node);
        if(update) {
            node->parameters = update->parameters;
            node->num_parameters = update->num_parameters;
            if(update->expected_data_types) {
                for(size_t i = 0; i < node->num_parameters; i++) {
                    if(update->expected_data_types[i] != SQL_TYPE_UNKNOWN &&
                       node->parameters[i]->data_type != update->expected_data_types[i]) {
                        node->parameters[i] = create_convert_node(context, node->parameters[i], update->expected_data_types[i]);
                        node->parameters[i]->data_type = update->expected_data_types[i];
                    }
                }
            }
            node->data_type = update->return_type;
            node->func = update->implementation;
        }
    }

    return node;
}

sql_node_t *sql_convert(sql_ctx_t *context, sql_node_t *param, sql_data_type_t target_type) {
    if (param->data_type == target_type) {
        return param;
    }
    return create_convert_node(context, param, target_type);
}

static sql_data_type_t determine_common_type(sql_data_type_t type1, sql_data_type_t type2) {
    if (type1 == type2) {
        return type1;
    }
    if ((type1 == SQL_TYPE_INT && type2 == SQL_TYPE_DOUBLE) ||
        (type1 == SQL_TYPE_DOUBLE && type2 == SQL_TYPE_INT)) {
        return SQL_TYPE_DOUBLE;
    }
    if(type1 == SQL_TYPE_DATETIME || type2 == SQL_TYPE_DATETIME) {
        return SQL_TYPE_DATETIME;
    }
    return SQL_TYPE_STRING;
}


void apply_type_conversions(sql_ctx_t *context, sql_node_t *node) {
    aml_pool_t *pool = context->pool;
    if (!node) {
        return;
    }

    for (size_t i = 0; i < node->num_parameters; i++) {
        apply_type_conversions(context, node->parameters[i]);
    }

    if ((node->token_type == SQL_OPERATOR || node->token_type == SQL_COMPARISON) && node->num_parameters == 2) {
        sql_node_t *left = node->parameters[0];
        sql_node_t *right = node->parameters[1];
        sql_data_type_t left_type = left->data_type;
        sql_data_type_t right_type = right->data_type;

        bool should_check = true;
        if (left_type == SQL_TYPE_DATETIME && right->type == SQL_COMPOUND_LITERAL) {
            const char *value = right->token;
            if (strncasecmp(value, "INTERVAL", 8) == 0) {
                should_check = false;
            }
        }
        if (!strcmp(node->token, "::")) {
            should_check = false;
        }

        if (node->token_type == SQL_OPERATOR && (left_type == SQL_TYPE_DATETIME || right_type == SQL_TYPE_DATETIME)) {
            should_check = false;
        }

        if (should_check && left_type != right_type) {
            if ((left->type == SQL_IDENTIFIER || left->type == SQL_FUNCTION) && is_literal(right)) {
                right = create_convert_node(context, right, left_type);
                right->data_type = left_type;
                node->parameters[1] = right;
            } else if (is_literal(left) && (right->type == SQL_IDENTIFIER || right->type == SQL_FUNCTION)) {
                left = create_convert_node(context, left, right_type);
                left->data_type = right_type;
                node->parameters[0] = left;
            } else {
                sql_data_type_t common_type = determine_common_type(left_type, right_type);
                if (left_type != common_type) {
                    left = create_convert_node(context, left, common_type);
                    left->data_type = common_type;
                    node->parameters[0] = left;
                }
                if (right_type != common_type) {
                    right = create_convert_node(context, right, common_type);
                    right->data_type = common_type;
                    node->parameters[1] = right;
                }
            }
        }
    }

    if (node->token_type == SQL_FUNCTION || node->token_type == SQL_COMPARISON || node->token_type == SQL_OPERATOR ||
        node->token_type == SQL_AND || node->token_type == SQL_OR || node->token_type == SQL_NOT) {

        if(node->spec) {
            sql_ctx_spec_t *spec = node->spec;
            if(spec && spec->update) {
                sql_ctx_spec_update_t *update = spec->update(context, spec, node);
                if(update) {
                    node->parameters = update->parameters;
                    node->num_parameters = update->num_parameters;
                    if(update->expected_data_types) {
                        for(size_t i = 0; i < node->num_parameters; i++) {
                            if(update->expected_data_types[i] != SQL_TYPE_UNKNOWN &&
                               node->parameters[i]->data_type != update->expected_data_types[i]) {
                                node->parameters[i] = create_convert_node(context, node->parameters[i], update->expected_data_types[i]);
                                node->parameters[i]->data_type = update->expected_data_types[i];
                            }
                        }
                    }
                    node->data_type = update->return_type;
                    node->func = update->implementation;
                }
            }
        }
    }
}

static sql_data_type_t parse_data_type_from_string(const char *type_str) {
    if (strcasecmp(type_str, "INT") == 0 || strcasecmp(type_str, "INTEGER") == 0) {
        return SQL_TYPE_INT;
    } else if (strcasecmp(type_str, "DOUBLE") == 0 || strcasecmp(type_str, "DECIMAL") == 0 || strcasecmp(type_str, "NUMERIC") == 0) {
        return SQL_TYPE_DOUBLE;
    } else if (strcasecmp(type_str, "STRING") == 0 || strcasecmp(type_str, "VARCHAR") == 0 || strcasecmp(type_str, "CHAR") == 0) {
        return SQL_TYPE_STRING;
    } else if (strcasecmp(type_str, "DATETIME") == 0) {
        return SQL_TYPE_DATETIME;
    } else if (strcasecmp(type_str, "BOOL") == 0 || strcasecmp(type_str, "BOOLEAN") == 0) {
        return SQL_TYPE_BOOL;
    }
    return SQL_TYPE_UNKNOWN;
}

void simplify_tree(sql_ctx_t *ctx, sql_node_t *node) {
    if (!node) return;

    if (node->num_parameters == 0 && !node->func) {
        return;
    }

    for (size_t i = 0; i < node->num_parameters; i++) {
        simplify_tree(ctx, node->parameters[i]);
    }

    bool all_literals = true;
    for (size_t i = 0; i < node->num_parameters; i++) {
        if (!is_literal(node->parameters[i])) {
            all_literals = false;
            break;
        }
    }

    if (all_literals && node->func && (node->spec || ctx->row)) {
        // --- FIX: Respect Volatility ---
        if (node->spec && node->spec->is_volatile) {
            // Do nothing. Leave volatile functions alone so they execute per-row.
        } else {
            sql_node_t *result_node = node->func(ctx, node);
            if (result_node) {
                *node = *result_node;
            }
        }
    }

    sql_token_type_t node_type = node->token_type;

    if (node_type == SQL_AND) {
        for (size_t i = 0; i < node->num_parameters; i++) {
            if (is_literal(node->parameters[i]) &&
                node->parameters[i]->data_type == SQL_TYPE_BOOL &&
                !node->parameters[i]->value.bool_value) {
                node->token = "FALSE";
                node->type = node->parameters[i]->type;
                node->token_type = node->parameters[i]->token_type;
                node->func = NULL;
                node->num_parameters = 0;
                node->parameters = NULL;
                node->data_type = SQL_TYPE_BOOL;
                node->value.bool_value = false;
                node->is_null = false;
                return;
            }
        }

        size_t write_index = 0;
        for (size_t i = 0; i < node->num_parameters; i++) {
            if (!(is_literal(node->parameters[i]) &&
                  node->parameters[i]->data_type == SQL_TYPE_BOOL &&
                  node->parameters[i]->value.bool_value)) {
                node->parameters[write_index++] = node->parameters[i];
            }
        }
        node->num_parameters = write_index;

        if (node->num_parameters == 1) {
            *node = *node->parameters[0];
        }
    } else if (node_type == SQL_OR) {
        for (size_t i = 0; i < node->num_parameters; i++) {
            if (is_literal(node->parameters[i]) &&
                node->parameters[i]->data_type == SQL_TYPE_BOOL &&
                node->parameters[i]->value.bool_value) {
                node->token = "TRUE";
                node->type = node->parameters[i]->type;
                node->token_type = node->parameters[i]->token_type;
                node->func = NULL;
                node->num_parameters = 0;
                node->parameters = NULL;
                node->data_type = SQL_TYPE_BOOL;
                node->value.bool_value = true;
                node->is_null = false;
                return;
            }
        }

        size_t write_index = 0;
        for (size_t i = 0; i < node->num_parameters; i++) {
            if (!(is_literal(node->parameters[i]) &&
                  node->parameters[i]->data_type == SQL_TYPE_BOOL &&
                  !node->parameters[i]->value.bool_value)) {
                node->parameters[write_index++] = node->parameters[i];
            }
        }
        node->num_parameters = write_index;

        if (node->num_parameters == 1) {
            *node = *node->parameters[0];
        }
    }
}

sql_node_t *copy_nodes(sql_ctx_t *ctx, sql_node_t *node) {
    if (!node) {
        return NULL;
    }

    sql_node_t *new_node = (sql_node_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_node_t));
    *new_node = *node;
    if(node->num_parameters) {
        new_node->parameters = (sql_node_t **)aml_pool_alloc(ctx->pool, node->num_parameters * sizeof(sql_node_t *));
        for (size_t i = 0; i < node->num_parameters; i++) {
            new_node->parameters[i] = copy_nodes(ctx, node->parameters[i]);
        }
    }
    return new_node;
}

void simplify_func_tree(sql_ctx_t *ctx, sql_node_t *node ) {
    if (!node || (node->num_parameters == 0 && !node->func)) {
        return;
    }

    for (size_t i = 0; i < node->num_parameters; i++) {
        simplify_func_tree(ctx, node->parameters[i]);
    }

    bool all_literals = true;
    for (size_t i = 0; i < node->num_parameters; i++) {
        if (!is_literal(node->parameters[i])) {
            all_literals = false;
            break;
        }
    }

    if (all_literals && node->func && (node->spec || ctx->row)) {
        // --- FIX: Respect Volatility ---
        if (node->spec && node->spec->is_volatile) {
            // Do nothing. Leave volatile functions alone so they execute per-row.
        } else {
            sql_node_t *result_node = node->func(ctx, node);

            if (result_node) {
                *node = *result_node;
            }
        }
    }
}

void simplify_logical_expressions(sql_node_t *node) {
    if (!node || node->num_parameters == 0) {
        return;
    }

    for (size_t i = 0; i < node->num_parameters; i++) {
        simplify_logical_expressions(node->parameters[i]);
    }

    sql_token_type_t node_type = node->token_type;

    if (node_type == SQL_AND) {
        for (size_t i = 0; i < node->num_parameters; i++) {
            if (is_literal(node->parameters[i]) &&
                node->parameters[i]->data_type == SQL_TYPE_BOOL &&
                !node->parameters[i]->value.bool_value) {
                node->token = "FALSE";
                node->type = node->parameters[i]->type;
                node->token_type = node->parameters[i]->token_type;
                node->func = NULL;
                node->num_parameters = 0;
                node->parameters = NULL;
                node->data_type = SQL_TYPE_BOOL;
                node->value.bool_value = false;
                node->is_null = false;
                return;
            }
        }

        size_t write_index = 0;
        for (size_t i = 0; i < node->num_parameters; i++) {
            if (!(is_literal(node->parameters[i]) &&
                  node->parameters[i]->data_type == SQL_TYPE_BOOL &&
                  node->parameters[i]->value.bool_value)) {
                node->parameters[write_index++] = node->parameters[i];
            }
        }
        node->num_parameters = write_index;

        if (node->num_parameters == 1) {
            *node = *node->parameters[0];
        }
    }

    else if (node_type == SQL_OR) {
        for (size_t i = 0; i < node->num_parameters; i++) {
            if (is_literal(node->parameters[i]) &&
                node->parameters[i]->data_type == SQL_TYPE_BOOL &&
                node->parameters[i]->value.bool_value) {
                node->token = "TRUE";
                node->type = node->parameters[i]->type;
                node->token_type = node->parameters[i]->token_type;
                node->func = NULL;
                node->num_parameters = 0;
                node->parameters = NULL;
                node->data_type = SQL_TYPE_BOOL;
                node->value.bool_value = true;
                node->is_null = false;
                return;
            }
        }

        size_t write_index = 0;
        for (size_t i = 0; i < node->num_parameters; i++) {
            if (!(is_literal(node->parameters[i]) &&
                  node->parameters[i]->data_type == SQL_TYPE_BOOL &&
                  !node->parameters[i]->value.bool_value)) {
                node->parameters[write_index++] = node->parameters[i];
            }
        }
        node->num_parameters = write_index;

        if (node->num_parameters == 1) {
            *node = *node->parameters[0];
        }
    }
}

void print_node(sql_ctx_t *ctx, sql_node_t *node, int depth) {
    if (!node) {
        return;
    }

    for (int i = 0; i < depth; i++) printf("  ");

    const char *type_name = sql_token_type_name(node->type);
    const char *data_type_name = sql_data_type_name(node->data_type);
    const char *value = node->token;
    if(node->token_type != SQL_IDENTIFIER && node->token_type != SQL_FUNCTION && node->token_type != SQL_COMPARISON && node->token_type != SQL_OPERATOR && node->data_type == SQL_TYPE_DATETIME) {
        value = convert_epoch_to_iso_utc(ctx->pool, node->value.epoch);
    }
    const char *func_name = sql_ctx_get_callback_name(ctx, node->func);
    if(!func_name)
        func_name = "NULL";
    printf("Type: %s, Value: %s, DataType: %s, Func: %s, %p\n", type_name, value, data_type_name, func_name,
            node->spec);

    for (size_t i = 0; i < node->num_parameters; i++) {
        print_node(ctx, node->parameters[i], depth + 1);
    }
}

sql_node_t *sql_eval(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->func)
        return f->func(ctx, f);
    return f;
}

sql_node_t *sql_bool_init(sql_ctx_t *ctx, bool value, bool is_null) {
    sql_node_t *result = (sql_node_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_node_t));
    result->func = NULL;
    result->num_parameters = 0;
    result->parameters = NULL;
    result->data_type = SQL_TYPE_BOOL;
    result->type = SQL_LITERAL;
    result->token_type = SQL_LITERAL;
    result->token = value ? aml_pool_strdup(ctx->pool, "true") : aml_pool_strdup(ctx->pool, "false");
    result->value.bool_value = value;
    result->is_null = is_null;
    return result;
}

sql_node_t *sql_list_init(sql_ctx_t *ctx, size_t num_elements, bool is_null) {
    sql_node_t *result = (sql_node_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_node_t));
    result->func = NULL;
    result->num_parameters = num_elements;
    result->parameters = (sql_node_t **)aml_pool_alloc(ctx->pool, num_elements * sizeof(sql_node_t *));
    result->data_type = SQL_TYPE_UNKNOWN;
    result->type = SQL_LIST;
    result->token_type = SQL_LIST;
    result->token = NULL;
    result->is_null = is_null;
    return result;
}

sql_node_t *sql_int_init(sql_ctx_t *ctx, int value, bool is_null) {
    sql_node_t *result = (sql_node_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_node_t));
    result->func = NULL;
    result->num_parameters = 0;
    result->parameters = NULL;
    result->data_type = SQL_TYPE_INT;
    result->type = SQL_LITERAL;
    result->token_type = SQL_LITERAL;
    result->token = aml_pool_strdupf(ctx->pool, "%d", value);
    result->value.int_value = value;
    result->is_null = is_null;
    return result;
}

sql_node_t *sql_double_init(sql_ctx_t *ctx, double value, bool is_null) {
    sql_node_t *result = (sql_node_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_node_t));
    result->func = NULL;
    result->num_parameters = 0;
    result->parameters = NULL;
    result->data_type = SQL_TYPE_DOUBLE;
    result->type = SQL_LITERAL;
    result->token_type = SQL_LITERAL;
    result->token = aml_pool_strdupf(ctx->pool, "%f", value);
    result->value.double_value = value;
    result->is_null = is_null;
    return result;
}

sql_node_t *sql_string_init(sql_ctx_t *ctx, const char *value, bool is_null) {
    if(!value) value = "";
    sql_node_t *result = (sql_node_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_node_t));
    result->func = NULL;
    result->num_parameters = 0;
    result->parameters = NULL;
    result->data_type = SQL_TYPE_STRING;
    result->type = SQL_LITERAL;
    result->token_type = SQL_LITERAL;
    result->token = aml_pool_strdup(ctx->pool, value);
    result->value.string_value = value;
    result->is_null = is_null;
    return result;
}

sql_node_t *sql_compound_init(sql_ctx_t *ctx, const char *value, bool is_null) {
    sql_node_t *result = (sql_node_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_node_t));
    result->func = NULL;
    result->num_parameters = 0;
    result->parameters = NULL;
    result->data_type = SQL_TYPE_STRING;
    result->type = SQL_COMPOUND_LITERAL;
    result->token_type = SQL_COMPOUND_LITERAL;
    result->token = aml_pool_strdup(ctx->pool, value);
    result->value.string_value = value;
    result->is_null = is_null;
    return result;
}


sql_node_t *sql_datetime_init(sql_ctx_t *ctx, time_t epoch, bool is_null) {
    sql_node_t *result = (sql_node_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_node_t));
    result->func = NULL;
    result->num_parameters = 0;
    result->parameters = NULL;
    result->data_type = SQL_TYPE_DATETIME;
    result->type = SQL_LITERAL;
    result->token_type = SQL_LITERAL;
    result->token = aml_pool_strdupf(ctx->pool, "%ld", epoch);
    result->value.epoch = epoch;
    result->is_null = is_null;
    return result;
}

sql_node_t *sql_function_init(sql_ctx_t *ctx, const char *name) {
    sql_node_t *result = (sql_node_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_node_t));
    result->func = NULL;
    result->num_parameters = 0;
    result->parameters = NULL;
    result->data_type = SQL_TYPE_UNKNOWN;
    result->type = SQL_FUNCTION;
    result->token_type = SQL_FUNCTION;
    result->token = aml_pool_strdup(ctx->pool, name);
    result->value.string_value = name;
    result->is_null = false;
    return result;
}
