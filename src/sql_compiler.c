// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_compiler.h"
#include "sql-parser-library/sql_ast.h"
#include <string.h>

sql_node_t *sql_compile_expression(sql_ctx_t *ctx, sql_ast_node_t *ast) {
    if (!ast) return NULL;
    sql_node_t *node = convert_ast_to_node(ctx, ast);
    apply_type_conversions(ctx, node);
    simplify_func_tree(ctx, node);
    simplify_logical_expressions(node);
    return node;
}

static void collect_aggregates(sql_node_t *node, sql_node_t **agg_array, size_t *count) {
    if (!node) return;

    if (node->spec && node->spec->is_aggregate) {
        node->agg_index = *count;
        agg_array[*count] = node;
        (*count)++;
        return;
    }

    for (size_t i = 0; i < node->num_parameters; i++) {
        collect_aggregates(node->parameters[i], agg_array, count);
    }
}

sql_compiled_query_t *sql_compile_query(sql_ctx_t *ctx, sql_select_t *ast) {
    if (!ast) return NULL;

    sql_compiled_query_t *compiled = aml_pool_zalloc(ctx->pool, sizeof(sql_compiled_query_t));

    // 1. Projections
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

    // 2. Group By
    size_t num_groups = 0;
    sql_ast_node_t *curr_gb = ast->group_by;
    while (curr_gb) { num_groups++; curr_gb = curr_gb->next; }

    if (num_groups > 0) {
        compiled->num_group_keys = num_groups;
        compiled->group_exprs = aml_pool_alloc(ctx->pool, num_groups * sizeof(sql_node_t *));
        curr_gb = ast->group_by;
        for (size_t i = 0; i < num_groups; i++) {
            compiled->group_exprs[i] = sql_compile_expression(ctx, curr_gb);
            curr_gb = curr_gb->next;
        }
    }

    // --- NEW: 3. Compile HAVING ---
    compiled->having_filter = sql_compile_expression(ctx, ast->having_clause);

    // 4. Sorting
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
            compiled->sort_directions[i] = curr_ob->is_desc ? -1 : 1;
            curr_ob = curr_ob->next;
        }
    }

    // 5. Map the Aggregates (Now including HAVING!)
    sql_node_t *agg_buffer[128];
    size_t agg_count = 0;

    for (size_t i = 0; i < compiled->num_projections; i++) {
        collect_aggregates(compiled->projections[i], agg_buffer, &agg_count);
    }

    // --- NEW: Hunt for aggregates in the HAVING clause ---
    if (compiled->having_filter) {
        collect_aggregates(compiled->having_filter, agg_buffer, &agg_count);
    }

    for (size_t i = 0; i < compiled->num_sort_keys; i++) {
        collect_aggregates(compiled->sort_exprs[i], agg_buffer, &agg_count);
    }

    if (agg_count > 0) {
        compiled->num_aggregates = agg_count;
        compiled->agg_nodes = aml_pool_alloc(ctx->pool, agg_count * sizeof(sql_node_t *));
        for (size_t i = 0; i < agg_count; i++) {
            compiled->agg_nodes[i] = agg_buffer[i];
        }
    }

    return compiled;
}
