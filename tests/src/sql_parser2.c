// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include "sql-parser-library/sql_tokenizer.h"
#include "sql-parser-library/sql_ast.h"
#include "sql-parser-library/sql_func.h"
#include "sql-parser-library/sql_types.h"
#include "a-memory-library/aml_pool.h"

// Example row structure to simulate database rows
typedef struct {
    const char *column_name;
    sql_value_t value;  // Column value
} row_t;

// Example data: rows in a mock "table"
row_t rows[][4] = {
    {{"id", {.type = SQL_TYPE_INT, .data.int_value = 1}},
     {"name", {.type = SQL_TYPE_STRING, .data.string_value = "Alice"}},
     {"age", {.type = SQL_TYPE_INT, .data.int_value = 30}},
     {"created", {.type = SQL_TYPE_DATE, .data.date_value = {.epoch = 1700000000}}}},

    {{"id", {.type = SQL_TYPE_INT, .data.int_value = 2}},
     {"name", {.type = SQL_TYPE_STRING, .data.string_value = "Bob"}},
     {"age", {.type = SQL_TYPE_INT, .data.int_value = 25}},
     {"created", {.type = SQL_TYPE_DATE, .data.date_value = {.epoch = 1700005000}}}},
};

// Number of rows
const size_t row_count = sizeof(rows) / sizeof(rows[0]);

// Recursive AST evaluation function
sql_value_t *evaluate_ast(aml_pool_t *pool, sql_ast_node_t *node, row_t *row, size_t row_size) {
    if (!node) {
        printf( "Error: AST node is NULL\n");
        return NULL;
    }
    printf( "Debug: Evaluating AST node type %d, value: %s\n", node->type, node->value);
    switch (node->type) {
        case SQL_IDENTIFIER: {
            // Find the column in the current row
            for (size_t i = 0; i < row_size; i++) {
                if (strcasecmp(row[i].column_name, node->value) == 0) {
                    // Return the column value directly
                    sql_value_t *result = (sql_value_t *)aml_pool_alloc(pool, sizeof(sql_value_t));
                    if (!result) {
                        printf( "Error: Memory allocation failed for column value\n");
                        return NULL;
                    }
                    *result = row[i].value; // Copy the column value
                    result->ops = get_ops_for_data_type(result->type);
                    return result;
                }
            }
            printf( "Error: Column '%s' not found\n", node->value);
            return NULL;
        }
        case SQL_NULL: {
            // Create a NULL SQL value
            return null_init(pool);
        }
        case SQL_OPERATOR: {
            // Evaluate the left and right operands
            sql_value_t *left_value = evaluate_ast(pool, node->left, row, row_size);
            sql_value_t *right_value = evaluate_ast(pool, node->right, row, row_size);

            if (!left_value || !right_value) {
                printf( "Error: Failed to evaluate operator operands\n");
                return NULL;
            }

            sql_value_t *result = (sql_value_t *)aml_pool_alloc(pool, sizeof(sql_value_t));
            if (!result) {
                printf( "Error: Memory allocation failed for operator result\n");
                return NULL;
            }

            // Perform the operation based on the operator type
            if (strcasecmp(node->value, "+") == 0 && left_value->ops.add) {
                result = left_value->ops.add(pool, left_value, right_value);
            } else if (strcasecmp(node->value, "-") == 0 && left_value->ops.subtract) {
                result = left_value->ops.subtract(pool, left_value, right_value);
            } else if (strcasecmp(node->value, "*") == 0 && left_value->ops.multiply) {
                result = left_value->ops.multiply(pool, left_value, right_value);
            } else if (strcasecmp(node->value, "/") == 0 && left_value->ops.divide) {
                result = left_value->ops.divide(pool, left_value, right_value);
            } else {
                printf( "Error: Unsupported operator '%s'\n", node->value);
                return NULL;
            }

            return result;
        }

        case SQL_NUMBER: {
            // Create a temporary SQL value for the number
            sql_value_t *result = (sql_value_t *)aml_pool_alloc(pool, sizeof(sql_value_t));
            if (!result) {
                printf( "Error: Memory allocation failed for number literal\n");
                return NULL;
            }
            result->type = SQL_TYPE_INT;
            result->data.int_value = atoi(node->value);
            result->ops = int_ops;
            return result;
        }

        case SQL_LITERAL: {
            // Create a temporary SQL value for the literal
            sql_value_t *result = (sql_value_t *)aml_pool_alloc(pool, sizeof(sql_value_t));
            if (!result) {
                printf( "Error: Memory allocation failed for string literal\n");
                return NULL;
            }
            result->type = SQL_TYPE_STRING;
            result->data.string_value = node->value;
            result->ops = string_ops;
            return result;
        }

        case SQL_COMPARISON: {
            sql_value_t *left_value = evaluate_ast(pool, node->left, row, row_size);

            if (!left_value) {
                printf( "Error: Failed to evaluate comparison left operand\n");
                return NULL;
            }

            if (strcasecmp(node->value, "IS NULL") == 0 || strcasecmp(node->value, "IS NOT NULL") == 0) {
                sql_value_t *result = bool_init(pool, false);
                if (left_value->is_null) {
                    result->data.bool_value = strcasecmp(node->value, "IS NULL") == 0;
                } else {
                    result->data.bool_value = strcasecmp(node->value, "IS NOT NULL") == 0;
                }
                return result;
            } else if (strcasecmp(node->value, "in") == 0 || strcasecmp(node->value, "NOT in") == 0) {
                // Handle IN or NOT IN
                sql_ast_node_t *current_value = node->right->left; // Start of the list
                sql_value_t *result = bool_init(pool, false); // Default: not in the list

                while (current_value) {
                    sql_value_t *list_value = evaluate_ast(pool, current_value, row, row_size);
                    if (!list_value) {
                        printf( "Error: Failed to evaluate IN list value\n");
                        return NULL;
                    }

                    // Compare column value with the current list value
                    if (left_value->ops.compare(pool, left_value, list_value) == 0) {
                        result->data.bool_value = true; // Match found
                        break;
                    }
                    current_value = current_value->next; // Move to the next value
                }

                // Negate result for NOT IN
                if (strcasecmp(node->value, "NOT in") == 0) {
                    result->data.bool_value = !result->data.bool_value;
                }

                return result;
            }
            else if (strcasecmp(node->value, "BETWEEN") == 0 || strcasecmp(node->value, "NOT BETWEEN") == 0) {
                // Evaluate the left expression
                sql_value_t *left_value = evaluate_ast(pool, node->left, row, row_size);
                if (!left_value) {
                    printf("Error: Failed to evaluate BETWEEN left operand\n");
                    return NULL;
                }

                // Evaluate lower and upper bounds
                sql_ast_node_t *bounds_node = node->right;
                sql_value_t *lower_bound = evaluate_ast(pool, bounds_node->left, row, row_size);
                sql_value_t *upper_bound = evaluate_ast(pool, bounds_node->right, row, row_size);

                if (!lower_bound || !upper_bound) {
                    printf("Error: Failed to evaluate BETWEEN bounds\n");
                    return NULL;
                }

                // Compare left_value with bounds
                int cmp_lower = left_value->ops.compare(pool, left_value, lower_bound);
                int cmp_upper = left_value->ops.compare(pool, left_value, upper_bound);

                bool between_result = (cmp_lower >= 0 && cmp_upper <= 0);

                // Negate result for NOT BETWEEN
                if (strcasecmp(node->value, "NOT BETWEEN") == 0) {
                    between_result = !between_result;
                }

                return bool_init(pool, between_result);
            }

            // Default handling for other comparison operators
            sql_value_t *right_value = evaluate_ast(pool, node->right, row, row_size);
            if (!right_value) {
                printf( "Error: Failed to evaluate comparison right operand\n");
                return NULL;
            }

            int cmp_result = left_value->ops.compare(pool, left_value, right_value);
            sql_value_t *result = (sql_value_t *)aml_pool_alloc(pool, sizeof(sql_value_t));
            if (!result) {
                printf( "Error: Memory allocation failed for comparison result\n");
                return NULL;
            }

            result->type = SQL_TYPE_BOOL;
            result->is_null = false; // Set default

            if (cmp_result == -2) {
                // Comparison involved NULL; result is NULL
                result->is_null = true;
            } else if (strcasecmp(node->value, "=") == 0) {
                result->data.bool_value = (cmp_result == 0);
            } else if (strcasecmp(node->value, "<=") == 0) {
                result->data.bool_value = (cmp_result <= 0);
            } else if (strcasecmp(node->value, "<") == 0) {
                result->data.bool_value = (cmp_result < 0);
            } else {
                printf( "Error: Unsupported comparison operator '%s'\n", node->value);
                return NULL;
            }

            return result;
        }
        case SQL_NOT: {
            sql_value_t *operand = evaluate_ast(pool, node->right, row, row_size);
            if (!operand) {
                printf("Error: Failed to evaluate NOT operand\n");
                return NULL;
            }

            if (operand->is_null) {
                sql_value_t *result = bool_init(pool, false);
                result->is_null = true;
                return result;
            }

            return bool_init(pool, !operand->data.bool_value);
        }

        case SQL_AND: {
            sql_value_t *left_result = evaluate_ast(pool, node->left, row, row_size);
            sql_value_t *right_result = evaluate_ast(pool, node->right, row, row_size);

            if (!left_result || !right_result) {
                printf("Error: Failed to evaluate AND operands\n");
                return NULL;
            }

            sql_value_t *result = bool_init(pool, false);
            if (left_result->is_null || right_result->is_null) {
                result->is_null = true;
            }

            if (!left_result->is_null && !left_result->data.bool_value) {
                result->data.bool_value = false;
                result->is_null = false;
            } else if (!right_result->is_null && !right_result->data.bool_value) {
                result->data.bool_value = false;
                result->is_null = false;
            } else if (!left_result->is_null && !right_result->is_null) {
                result->data.bool_value = true;
                result->is_null = false;
            } else {
                result->is_null = true;
            }

            return result;
        }

        case SQL_OR: {
            sql_value_t *left_result = evaluate_ast(pool, node->left, row, row_size);
            sql_value_t *right_result = evaluate_ast(pool, node->right, row, row_size);

            if (!left_result || !right_result) {
                printf("Error: Failed to evaluate OR operands\n");
                return NULL;
            }

            sql_value_t *result = bool_init(pool, false);
            if (left_result->is_null || right_result->is_null) {
                result->is_null = true;
            }

            if (!left_result->is_null && left_result->data.bool_value) {
                result->data.bool_value = true;
                result->is_null = false;
            } else if (!right_result->is_null && right_result->data.bool_value) {
                result->data.bool_value = true;
                result->is_null = false;
            } else if (!left_result->is_null && !right_result->is_null) {
                result->data.bool_value = false;
                result->is_null = false;
            } else {
                result->is_null = true;
            }

            return result;
        }

        default: {
            printf( "Error: Unsupported AST node type\n");
            return NULL;
        }
    }
}

// Filter rows based on the expression
void apply_expression(aml_pool_t *pool, sql_ast_node_t *expr_node) {
    for (size_t i = 0; i < row_count; i++) {
        printf("Debug: Evaluating row %zu:\n", i);
        for (size_t j = 0; j < 4; j++) { // Assuming each row has 4 columns
            printf("  Column %s = ", rows[i][j].column_name);
            switch (rows[i][j].value.type) {
                case SQL_TYPE_INT:
                    printf("%d\n", rows[i][j].value.data.int_value);
                    break;
                case SQL_TYPE_STRING:
                    printf("%s\n", rows[i][j].value.data.string_value);
                    break;
                case SQL_TYPE_DATE:
                    printf("%ld\n", rows[i][j].value.data.date_value.epoch);
                    break;
                default:
                    printf("Unknown\n");
            }
        }

        sql_value_t *match = evaluate_ast(pool, expr_node, rows[i], 4);
        if (match && match->type == SQL_TYPE_BOOL && match->data.bool_value) {
            printf("Row %zu matches the expression\n", i);
        } else {
            printf("Row %zu does not match the expression\n", i);
        }
    }
}

int main(int argc, char **argv) {
    if (argc < 2) {
        printf( "Usage: %s \"Expression\"\n", argv[0]);
        return 1;
    }

    sql_column_t columns[] = {
        {"id", SQL_TYPE_INT, int_ops},
        {"name", SQL_TYPE_STRING, string_ops},
        {"age", SQL_TYPE_INT, int_ops},
        {"created", SQL_TYPE_DATE, date_ops}
    };
    size_t column_count = sizeof(columns) / sizeof(columns[0]);

    aml_pool_t *pool = aml_pool_init(1024);
    size_t token_count;
    sql_token_t **tokens = sql_tokenize(pool, argv[1], &token_count);

    printf(">> Tokens:\n\n");
    sql_tokenizer_print(tokens, token_count);

    sql_ast_node_t *ast = build_ast(pool, tokens, token_count, columns, column_count);

    printf("\n\n>> AST Tree:\n\n");
    print_ast(ast, 0);

    if (ast) {
        sql_func_node_t *func_node = convert_ast_to_func_node(pool, ast);
        apply_type_conversions(pool, func_node);
        printf("\n\n>> Expression as function tree:\n\n");
        print_func_node(func_node, 0);
    }

    printf("\n\n>> Applying Expression:\n\n");
    if (ast) {
        apply_expression(pool, ast);
    } else {
        printf("No expression found.\n");
    }

    aml_pool_destroy(pool);
    return 0;
}
