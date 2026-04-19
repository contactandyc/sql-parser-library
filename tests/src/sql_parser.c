// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include "sql-parser-library/sql_tokenizer.h"
#include "sql-parser-library/sql_ast.h"
#include "sql-parser-library/sql_ctx.h"
#include "a-memory-library/aml_pool.h"

// Example row structure to simulate database rows
typedef struct {
    const char *row_data;
} row_t;

row_t rows[] = {
    {"1, 'Alice', 25, '2021-01-01'"},
    {"2, 'Bob', 30, '2021-01-02'"},
    {"3, 'Charlie', 35, '2021-01-03'"},
    {"4, 'David', 40, '2021-01-04'"},
    {"5, 'Eve', 45, '2021-01-05'"},
};

// Number of rows
const size_t row_count = sizeof(rows) / sizeof(rows[0]);

sql_node_t *sql_func_get_data(sql_ctx_t *ctx, sql_node_t *f) {
    return NULL; // Implement actual data retrieval
}

sql_ctx_spec_t *create_function_specs(size_t *num_specs, aml_pool_t *pool) {
    // Initialize and return the function specs
    *num_specs = 0;
    return NULL;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        printf("Usage: %s \"Expression\"\n", argv[0]);
        return 1;
    }

    sql_ctx_column_t columns[] = {
        {"id", SQL_TYPE_INT, sql_func_get_data},
        {"name", SQL_TYPE_STRING, sql_func_get_data},
        {"age", SQL_TYPE_INT, sql_func_get_data},
        {"created", SQL_TYPE_DATETIME, sql_func_get_data},
        {"STRING", SQL_TYPE_STRING, sql_func_get_data},
        {"documents", SQL_TYPE_STRING, sql_func_get_data}
    };

    size_t column_count = sizeof(columns) / sizeof(columns[0]);
    aml_pool_t *pool = aml_pool_init(1024);

    sql_ctx_t context = {0};
    context.pool = pool;
    context.columns = columns;
    context.column_count = column_count;
    register_ctx(&context);

    size_t token_count;
    sql_token_t **tokens = sql_tokenize(&context, argv[1], &token_count);

    printf(">> Tokens:\n\n");
    sql_token_print(tokens, token_count);

    sql_ast_node_t *ast = build_ast(&context, tokens, token_count);
    printf("\n\n>> AST Tree:\n\n");

    if (ast) {
        print_ast(ast, 0);
        sql_node_t *func_node = convert_ast_to_node(&context, ast);
        printf("\n\n>> Expression as function tree before type conversions:\n\n");
        print_node(&context, func_node, 0);
        apply_type_conversions(&context, func_node);
        printf("\n\n>> Expression as function tree before simplification:\n\n");
        print_node(&context, func_node, 0);
        simplify_func_tree(&context, func_node);
        printf("\n\n>> Expression as function tree after simplification:\n\n");
        print_node(&context, func_node, 0);
        simplify_logical_expressions(func_node);
        printf("\n\n>> Expression as function tree after logical simplification:\n\n");
        print_node(&context, func_node, 0);
    }

    printf("\n\n\n");
    sql_ctx_print_messages(&context);

    aml_pool_destroy(pool);
    return 0;
}
