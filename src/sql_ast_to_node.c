// SPDX-FileCopyrightText: 2024-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai — technical questions: contact Andy (above)
// SPDX-License-Identifier: Apache-2.0

#include "sql-parser-library/sql_ast.h"
#include "sql-parser-library/sql_node.h"
#include "sql-parser-library/sql_ctx.h"
#include "a-memory-library/aml_pool.h"
#include "sql-parser-library/date_utils.h"
#include "sql-parser-library/sql_tokenizer.h" // For token type names

#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>

sql_data_type_t sql_determine_common_type(sql_data_type_t type1, sql_data_type_t type2) {
    if (type1 == type2) return type1;

    // Handle unknown types gracefully
    if (type1 == SQL_TYPE_UNKNOWN) return type2;
    if (type2 == SQL_TYPE_UNKNOWN) return type1;

    // Promote INT to DOUBLE if mixed
    if ((type1 == SQL_TYPE_INT && type2 == SQL_TYPE_DOUBLE) ||
        (type1 == SQL_TYPE_DOUBLE && type2 == SQL_TYPE_INT))
        return SQL_TYPE_DOUBLE;

    // DATETIME should remain as DATETIME
    if ((type1 == SQL_TYPE_DATETIME && type2 == SQL_TYPE_INT) ||
        (type1 == SQL_TYPE_INT && type2 == SQL_TYPE_DATETIME) ||
        (type1 == SQL_TYPE_DATETIME && type2 == SQL_TYPE_DOUBLE) ||
        (type1 == SQL_TYPE_DOUBLE && type2 == SQL_TYPE_DATETIME))
        return SQL_TYPE_DATETIME;

    if(type1 == SQL_TYPE_DATETIME && type2 == SQL_TYPE_STRING)
        return SQL_TYPE_DATETIME;
    if(type1 == SQL_TYPE_STRING && type2 == SQL_TYPE_DATETIME)
        return SQL_TYPE_DATETIME;

    // If STRING is involved, default to STRING (not ideal for BETWEEN)
    if (type1 == SQL_TYPE_STRING || type2 == SQL_TYPE_STRING)
        return SQL_TYPE_STRING;

    // If incompatible types, return UNKNOWN
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
            node->value.string_value = aml_pool_strdup(pool, ast->value);
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
                if (convert_string_to_datetime(&epoch, pool, ast->value)) {
                    node->value.epoch = epoch;
                } else {
                    node->is_null = true;
                }
                break;
            }
        default:
            // Handle unknown types or set value to null
            node->is_null = true;
            break;
    }
}

sql_node_t *convert_ast_to_node(sql_ctx_t *context, sql_ast_node_t *ast) {
    aml_pool_t *pool = context->pool;
    if (!ast) {
        return NULL;
    }

    // Allocate memory for the sql_node_t
    sql_node_t *node = (sql_node_t *)aml_pool_zalloc(pool, sizeof(sql_node_t));
    // Populate the basic fields
    node->token_type = ast->type;
    node->token = (ast->type == SQL_LIST) ? NULL : aml_pool_strdup(pool, ast->value);
    node->type = ast->type;
    node->data_type = ast->data_type;
    node->spec = ast->spec;

    if (ast->type != SQL_LIST)
        convert_value(pool, ast, node);

    // Handle specific cases like BETWEEN and NOT BETWEEN
    if (ast->type == SQL_LIST) {
        node->data_type = infer_list_type(ast); // Infer list type dynamically
        size_t count = 0;
        sql_ast_node_t *elem = ast->left;
        while (elem) { count++; elem = elem->next; }

        node->num_parameters = count;
        node->parameters = (sql_node_t **)aml_pool_alloc(pool, count * sizeof(sql_node_t *));
        elem = ast->left;
        for (size_t i = 0; elem; i++, elem = elem->next) {
            node->parameters[i] = convert_ast_to_node(context, elem);
        }
    } else if (ast->spec && strcasecmp(ast->spec->name, "EXTRACT") == 0) {
        // Ensure EXTRACT has `left` (field) and `right` (FROM with source)
        if (ast->left && strcasecmp(ast->left->value, "FROM") == 0) {
            node->num_parameters = 2;
            node->parameters = (sql_node_t **)aml_pool_alloc(pool, 2 * sizeof(sql_node_t *));
            node->parameters[1] = convert_ast_to_node(context, ast->left->right);      // Source (e.g., a DATETIME)
            if(is_valid_extract(ast->left->left->value)) {
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
        // Parameters: expression, lower bound, upper bound
        node->parameters[0] = convert_ast_to_node(context, ast->left);
        node->parameters[1] = convert_ast_to_node(context, ast->right->left);  // Lower bound
        node->parameters[2] = convert_ast_to_node(context, ast->right->right); // Upper bound
    } else if (ast->type == SQL_COMPARISON && (strcasecmp(ast->value, "IS NULL") == 0 || strcasecmp(ast->value, "IS NOT NULL") == 0)) {
        node->num_parameters = 1;
        node->parameters = (sql_node_t **)aml_pool_alloc(pool, sizeof(sql_node_t *));
        node->parameters[0] = convert_ast_to_node(context, ast->left);
        // node->func = (strcasecmp(ast->value, "IS NULL") == 0) ? sql_func_is_null : sql_func_is_not_null;
        node->data_type = SQL_TYPE_BOOL; // Both IS NULL and IS NOT NULL return BOOL
    } else if(ast->type == SQL_IDENTIFIER) {
        for(size_t i = 0; i < context->column_count; i++) {
            if(strcasecmp(context->columns[i].name, ast->value) == 0) {
                node->data_type = context->columns[i].type;
                node->func = context->columns[i].func;
                break;
            }
        }
    } else {
        // Count the number of child nodes (parameters)
        size_t num_parameters = 0;
        sql_ast_node_t *child = ast->left;

        // Count all nodes in the chain linked via `next`
        if (ast->left && ast->right) {
            num_parameters = 2; // Binary node with exactly two children
        } else {
            child = ast->left;
            while (child) {
                num_parameters++;
                child = child->next;
            }
        }

        node->num_parameters = num_parameters;

        // Allocate memory for parameters array
        if (num_parameters > 0) {
            node->parameters = (sql_node_t **)aml_pool_alloc(pool, num_parameters * sizeof(sql_node_t *));

            // Recursively convert child nodes
            size_t index = 0;
            if (ast->left && ast->right) {
                // Handle binary nodes with left and right children
                node->parameters[index++] = convert_ast_to_node(context, ast->left);
                node->parameters[index++] = convert_ast_to_node(context, ast->right);
            } else if (ast->left) {
                // Traverse `next` chain for nodes using linked lists
                child = ast->left;
                while (child) {
                    node->parameters[index++] = convert_ast_to_node(context, child);
                    child = child->next;
                }
            }
        } else {
            node->parameters = NULL;
        }
    }

    return node;
}
