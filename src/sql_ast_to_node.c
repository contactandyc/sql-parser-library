// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ast.h"
#include "sql-parser-library/sql_node.h"
#include "sql-parser-library/sql_ctx.h"
#include "a-memory-library/aml_pool.h"
#include "sql-parser-library/date_utils.h"
#include "sql-parser-library/sql_tokenizer.h"

#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>

sql_data_type_t sql_determine_common_type(sql_data_type_t type1, sql_data_type_t type2) {
    if (type1 == type2) return type1;

    if (type1 == SQL_TYPE_UNKNOWN) return type2;
    if (type2 == SQL_TYPE_UNKNOWN) return type1;

    if ((type1 == SQL_TYPE_INT && type2 == SQL_TYPE_DOUBLE) ||
        (type1 == SQL_TYPE_DOUBLE && type2 == SQL_TYPE_INT))
        return SQL_TYPE_DOUBLE;

    if ((type1 == SQL_TYPE_DATETIME && type2 == SQL_TYPE_INT) ||
        (type1 == SQL_TYPE_INT && type2 == SQL_TYPE_DATETIME) ||
        (type1 == SQL_TYPE_DATETIME && type2 == SQL_TYPE_DOUBLE) ||
        (type1 == SQL_TYPE_DOUBLE && type2 == SQL_TYPE_DATETIME))
        return SQL_TYPE_DATETIME;

    if(type1 == SQL_TYPE_DATETIME && type2 == SQL_TYPE_STRING)
        return SQL_TYPE_DATETIME;
    if(type1 == SQL_TYPE_STRING && type2 == SQL_TYPE_DATETIME)
        return SQL_TYPE_DATETIME;

    if (type1 == SQL_TYPE_STRING || type2 == SQL_TYPE_STRING)
        return SQL_TYPE_STRING;

    return SQL_TYPE_UNKNOWN;
}

static sql_data_type_t infer_list_type(sql_ast_node_t *list) {
    if (!list || list->type != SQL_LIST) return SQL_TYPE_UNKNOWN;

    sql_data_type_t common_type = SQL_TYPE_UNKNOWN;
    sql_ast_node_t *current = list->left;

    while (current) {
        sql_data_type_t element_type = current->data_type;
        if (common_type == SQL_TYPE_UNKNOWN) {
            common_type = element_type;
        } else {
            common_type = sql_determine_common_type(common_type, element_type);
        }
        current = current->next;
    }
    return common_type;
}

static void convert_value(aml_pool_t *pool, sql_ast_node_t *ast, sql_node_t *node) {
    switch (ast->data_type) {
        case SQL_TYPE_INT:
            if(sscanf(ast->value, "%d", &node->value.int_value) != 1) {
                node->is_null = true;
            }
            break;
        case SQL_TYPE_DOUBLE:
            if(sscanf(ast->value, "%lf", &node->value.double_value) != 1) {
                node->is_null = true;
            }
            break;
        case SQL_TYPE_STRING:
            if (ast->type == SQL_COMPOUND_LITERAL && strncasecmp(ast->value, "INTERVAL", 8) == 0) {
                const char *val_ptr = ast->value + 8;
                while (isspace(*val_ptr)) val_ptr++;
                if (*val_ptr == '\'') val_ptr++;

                char *clean_val = aml_pool_strdup(pool, val_ptr);
                if (strlen(clean_val) > 0 && clean_val[strlen(clean_val)-1] == '\'') {
                    clean_val[strlen(clean_val)-1] = '\0';
                }
                node->value.string_value = clean_val;
            } else {
                node->value.string_value = aml_pool_strdup(pool, ast->value);
            }
            break;
        case SQL_TYPE_BOOL:
            if (strcasecmp(ast->value, "true") == 0 || strcmp(ast->value, "1") == 0) {
                node->value.bool_value = true;
            } else {
                node->value.bool_value = false;
            }
            break;
        case SQL_TYPE_DATETIME: {
                time_t epoch = 0;
                const char *val_ptr = ast->value;
                char *clean_val = NULL;

                if (ast->type == SQL_COMPOUND_LITERAL && strncasecmp(val_ptr, "TIMESTAMP", 9) == 0) {
                    val_ptr += 9;
                    while (isspace(*val_ptr)) val_ptr++;
                    if (*val_ptr == '\'') val_ptr++;

                    clean_val = aml_pool_strdup(pool, val_ptr);
                    if (strlen(clean_val) > 0 && clean_val[strlen(clean_val)-1] == '\'') {
                        clean_val[strlen(clean_val)-1] = '\0';
                    }
                    val_ptr = clean_val;
                }

                if (date_utils_convert_string_to_datetime(&epoch, pool, val_ptr)) {
                    node->value.epoch = epoch;
                } else {
                    node->is_null = true;
                }
                break;
            }
        default:
            node->is_null = true;
            break;
    }
}

sql_node_t *sql_convert_ast_to_node(sql_ctx_t *context, sql_ast_node_t *ast) {
    aml_pool_t *pool = context->pool;
    if (!ast) {
        return NULL;
    }

    sql_node_t *node = (sql_node_t *)aml_pool_zalloc(pool, sizeof(sql_node_t));

    node->token_type = ast->type;
    node->token = (ast->type == SQL_LIST || ast->type == SQL_NODE_SUBQUERY) ? NULL : aml_pool_strdup(pool, ast->value);
    node->type = ast->type;
    node->column = ast->column;
    node->data_type = ast->data_type;
    node->spec = ast->spec;

    if (ast->type == SQL_NODE_SUBQUERY || ast->type == SQL_EXISTS) {
        node->token = aml_pool_strdup(pool, ast->value ? ast->value :
                     (ast->type == SQL_EXISTS ? "EXISTS" : "SUBQUERY"));
        node->subquery_ast = ast->subquery;
    } else if (ast->type != SQL_LIST) {
        convert_value(pool, ast, node);
    }

    if (ast->type == SQL_LIST) {
        node->data_type = infer_list_type(ast);
        size_t count = 0;
        sql_ast_node_t *elem = ast->left;
        while (elem) { count++; elem = elem->next; }

        node->num_parameters = count;
        node->parameters = (sql_node_t **)aml_pool_alloc(pool, count * sizeof(sql_node_t *));
        elem = ast->left;
        for (size_t i = 0; elem; i++, elem = elem->next) {
            node->parameters[i] = sql_convert_ast_to_node(context, elem);
        }
    } else if (ast->spec && strcasecmp(ast->spec->name, "EXTRACT") == 0) {
        if (ast->left && strcasecmp(ast->left->value, "FROM") == 0) {
            node->num_parameters = 2;
            node->parameters = (sql_node_t **)aml_pool_alloc(pool, 2 * sizeof(sql_node_t *));
            node->parameters[1] = sql_convert_ast_to_node(context, ast->left->right);
            if(sql_is_valid_extract(ast->left->left->value)) {
                node->parameters[0] = sql_string_init(context, ast->left->left->value, false);
            } else {
                sql_ctx_error(NULL, "Invalid EXTRACT syntax: invalid field");
                node->is_null = true;
                node->num_parameters = 0;
            }
        } else {
            sql_ctx_error(NULL, "Invalid EXTRACT syntax: missing field or source");
            node->is_null = true;
        }
    } else if (ast->type == SQL_COMPARISON && (strcasecmp(ast->value, "BETWEEN") == 0 || strcasecmp(ast->value, "NOT BETWEEN") == 0)) {
        node->num_parameters = 3;
        node->parameters = (sql_node_t **)aml_pool_alloc(pool, 3 * sizeof(sql_node_t *));
        node->parameters[0] = sql_convert_ast_to_node(context, ast->left);
        node->parameters[1] = sql_convert_ast_to_node(context, ast->right->left);
        node->parameters[2] = sql_convert_ast_to_node(context, ast->right->right);
    } else if (ast->type == SQL_COMPARISON && (strcasecmp(ast->value, "IS NULL") == 0 || strcasecmp(ast->value, "IS NOT NULL") == 0)) {
        node->num_parameters = 1;
        node->parameters = (sql_node_t **)aml_pool_alloc(pool, sizeof(sql_node_t *));
        node->parameters[0] = sql_convert_ast_to_node(context, ast->left);
        node->data_type = SQL_TYPE_BOOL;
    } else if(ast->type == SQL_IDENTIFIER) {
        if (ast->column) {
            node->func = ast->column->func;
        } else if (context->schema_lookup) {
            sql_ctx_column_t *col = context->schema_lookup(context, NULL, ast->value);
            if (col) {
                node->data_type = col->type;
                node->func = col->func;
            } else {
                node->is_null = true;
            }
        } else {
            node->is_null = true;
        }
    } else {
        size_t num_parameters = 0;
        sql_ast_node_t *child = ast->left;

        if (ast->left && ast->right) {
            num_parameters = 2;
        } else {
            child = ast->left;
            while (child) {
                num_parameters++;
                child = child->next;
            }
        }

        node->num_parameters = num_parameters;

        if (num_parameters > 0) {
            node->parameters = (sql_node_t **)aml_pool_alloc(pool, num_parameters * sizeof(sql_node_t *));
            size_t index = 0;
            if (ast->left && ast->right) {
                node->parameters[index++] = sql_convert_ast_to_node(context, ast->left);
                node->parameters[index++] = sql_convert_ast_to_node(context, ast->right);
            } else if (ast->left) {
                child = ast->left;
                while (child) {
                    node->parameters[index++] = sql_convert_ast_to_node(context, child);
                    child = child->next;
                }
            }
        } else {
            node->parameters = NULL;
        }
    }

    return node;
}
