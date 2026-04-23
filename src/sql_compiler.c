// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_compiler.h"
#include "sql-parser-library/sql_ast.h" // Needed for convert_ast_to_node
#include <string.h>

sql_node_t *sql_compile_expression(sql_ctx_t *ctx, sql_ast_node_t *ast) {
    if (!ast) return NULL;
    sql_node_t *node = convert_ast_to_node(ctx, ast);
    apply_type_conversions(ctx, node);
    simplify_func_tree(ctx, node);
    simplify_logical_expressions(node);
    return node;
}

sql_compiled_query_t *sql_compile_query(sql_ctx_t *ctx, sql_select_t *ast) {
    if (!ast) return NULL;

    sql_compiled_query_t *compiled = aml_pool_zalloc(ctx->pool, sizeof(sql_compiled_query_t));

    // 1. Compile Projections (SELECT)
    size_t num_projs = 0;
    sql_ast_node_t *curr_col = ast->columns;
    while (curr_col) { num_projs++; curr_col = curr_col->next; }

    if (num_projs > 0) {
        compiled->num_projections = num_projs;
        compiled->projections = aml_pool_alloc(ctx->pool, num_projs * sizeof(sql_node_t *));
        compiled->display_names = aml_pool_alloc(ctx->pool, num_projs * sizeof(char *));

        curr_col = ast->columns;
        for (size_t i = 0; i < num_projs; i++) {
            compiled->projections[i] = sql_compile_expression(ctx, curr_col);
            compiled->display_names[i] = curr_col->alias ? curr_col->alias : (curr_col->value ? curr_col->value : "expr");
            curr_col = curr_col->next;
        }
    }

    // 2. Compile Sorting (ORDER BY)
    size_t num_sorts = 0;
    sql_order_by_t *curr_ob = ast->order_by;
    while (curr_ob) { num_sorts++; curr_ob = curr_ob->next; }

    if (num_sorts > 0) {
        compiled->num_sort_keys = num_sorts;
        compiled->sort_exprs = aml_pool_alloc(ctx->pool, num_sorts * sizeof(sql_node_t *));
        compiled->sort_directions = aml_pool_alloc(ctx->pool, num_sorts * sizeof(int));

        curr_ob = ast->order_by;
        for (size_t i = 0; i < num_sorts; i++) {
            compiled->sort_exprs[i] = sql_compile_expression(ctx, curr_ob->expr);
            // Default to 1 (ASC) for now, update to use parser logic when available
            compiled->sort_directions[i] = curr_ob->is_desc ? -1 : 1;
            curr_ob = curr_ob->next;
        }
    }

    return compiled;
}
