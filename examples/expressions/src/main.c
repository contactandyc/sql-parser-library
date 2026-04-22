// test_geo.c
// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include <stdio.h>
#include <stdlib.h>
#include "sql-parser-library/sql_tokenizer.h"
#include "sql-parser-library/sql_ast.h"
#include "sql-parser-library/sql_ctx.h"
#include "a-memory-library/aml_pool.h"

int main(int argc, char *argv[]) {
    aml_pool_t *pool = aml_pool_init(1024);

    // Initialize the SQL context
    sql_ctx_t ctx = {0};
    ctx.pool = pool;
    register_ctx(&ctx);

    // The expression to evaluate:
    // Columbus, OH (40.0, -83.0) to Cleveland, OH (41.5, -81.7)
    const char *expr = "GEO_DISTANCE(40.0, -83.0, 41.5, -81.7)";
    if(argc > 1)
       expr = argv[1];

    printf("Parsing expression: %s\n", expr);

    // 1. Tokenize
    size_t token_count = 0;
    sql_token_t **tokens = sql_tokenize(&ctx, expr, &token_count);
    if (!tokens || token_count == 0) {
        printf("Failed to tokenize.\n");
        return 1;
    }

    // 2. Build AST
    sql_ast_node_t *ast = build_ast(&ctx, tokens, token_count);
    if (!ast) {
        printf("Failed to build AST.\n");
        sql_ctx_print_messages(&ctx);
        return 1;
    }

    // 3. Convert to Execution Node
    sql_node_t *exec_node = convert_ast_to_node(&ctx, ast);
    if (!exec_node) {
        printf("Failed to convert to execution node.\n");
        return 1;
    }

    // 4. Apply Coercions & Simplifications
    apply_type_conversions(&ctx, exec_node);
    simplify_func_tree(&ctx, exec_node);

    // 5. Evaluate
    sql_node_t *result = sql_eval(&ctx, exec_node);

    // 6. Output Result
    if (result && !result->is_null && result->data_type == SQL_TYPE_DOUBLE) {
        printf("\nResult: %.2f miles\n", result->value.double_value);
    } else {
        printf("\nEvaluation failed or returned NULL.\n");
    }

    aml_pool_destroy(pool);
    return 0;
}
