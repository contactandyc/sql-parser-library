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

    if (node->spec && node->spec->is_aggregate && !node->spec->is_window_func) {
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

            if (curr_col->alias) {
                compiled->display_names[i] = curr_col->alias;
            } else if (curr_col->value) {
                const char *dot = strchr(curr_col->value, '.');
                compiled->display_names[i] = dot ? dot + 1 : curr_col->value;
            } else {
                compiled->display_names[i] = "expr";
            }
            curr_col = curr_col->next;
        }
    }

    // --- Map the Window Plans ---
    size_t win_count = 0;
    curr_col = ast->columns;
    while (curr_col) {
        if (curr_col->type == SQL_FUNCTION && curr_col->window_clause) win_count++;
        curr_col = curr_col->next;
    }

    if (win_count > 0) {
        compiled->num_window_plans = win_count;
        compiled->window_plans = aml_pool_alloc(ctx->pool, win_count * sizeof(sql_window_plan_t *));
        size_t w_idx = 0;

        curr_col = ast->columns;
        for (size_t i = 0; i < num_projs; i++) {
            if (curr_col->type == SQL_FUNCTION && curr_col->window_clause) {
                sql_window_plan_t *wp = aml_pool_zalloc(ctx->pool, sizeof(sql_window_plan_t));
                wp->projection_index = i;
                wp->func_node = compiled->projections[i];

                size_t n_part = 0;
                sql_ast_node_t *p = curr_col->window_clause->partition_by;
                while(p) { n_part++; p = p->next; }
                wp->num_partition_keys = n_part;
                wp->partition_exprs = aml_pool_alloc(ctx->pool, n_part * sizeof(sql_node_t*));

                p = curr_col->window_clause->partition_by;
                for(size_t k=0; k<n_part; k++) {
                    wp->partition_exprs[k] = sql_compile_expression(ctx, p);
                    p = p->next;
                }

                size_t n_sort = 0;
                sql_order_by_t *ob = curr_col->window_clause->order_by;
                while(ob) { n_sort++; ob = ob->next; }
                wp->num_sort_keys = n_sort;
                wp->sort_exprs = aml_pool_alloc(ctx->pool, n_sort * sizeof(sql_node_t*));
                wp->sort_directions = aml_pool_alloc(ctx->pool, n_sort * sizeof(int));

                ob = curr_col->window_clause->order_by;
                for(size_t k=0; k<n_sort; k++) {
                    wp->sort_exprs[k] = sql_compile_expression(ctx, ob->expr);
                    wp->sort_directions[k] = ob->is_desc ? -1 : 1;
                    ob = ob->next;
                }

                compiled->window_plans[w_idx++] = wp;
            }
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

    // 3. Compile HAVING
    compiled->having_filter = sql_compile_expression(ctx, ast->having_clause);

    // 4. Sorting (WITH ALIAS LINKING)
    size_t num_sorts = 0;
    sql_order_by_t *curr_ob = ast->order_by;
    while (curr_ob) { num_sorts++; curr_ob = curr_ob->next; }

    if (num_sorts > 0) {
        compiled->num_sort_keys = num_sorts;
        compiled->sort_exprs = aml_pool_alloc(ctx->pool, num_sorts * sizeof(sql_node_t *));
        compiled->sort_directions = aml_pool_alloc(ctx->pool, num_sorts * sizeof(int));
        compiled->sort_projection_indices = aml_pool_alloc(ctx->pool, num_sorts * sizeof(int));

        curr_ob = ast->order_by;
        for (size_t i = 0; i < num_sorts; i++) {
            compiled->sort_exprs[i] = sql_compile_expression(ctx, curr_ob->expr);
            compiled->sort_directions[i] = curr_ob->is_desc ? -1 : 1;
            compiled->sort_projection_indices[i] = -1;

            // Alias carried over from binder's projection injection
            if (curr_ob->expr->alias) {
                for (size_t p = 0; p < compiled->num_projections; p++) {
                    if (strcasecmp(curr_ob->expr->alias, compiled->display_names[p]) == 0) {
                        compiled->sort_projection_indices[i] = (int)p;
                        break;
                    }
                }
            }

            // Existing identifier-based fallback
            if (compiled->sort_projection_indices[i] == -1 &&
                curr_ob->expr->type == SQL_IDENTIFIER && curr_ob->expr->value) {

                const char *ob_name = curr_ob->expr->value;
                const char *dot = strchr(ob_name, '.');
                if (dot) ob_name = dot + 1;

                for (size_t p = 0; p < compiled->num_projections; p++) {
                    if (strcasecmp(ob_name, compiled->display_names[p]) == 0) {
                        compiled->sort_projection_indices[i] = (int)p;
                        break;
                    }
                }
            }

            curr_ob = curr_ob->next;
        }
    }

    // 5. Map the Aggregates
    sql_node_t *agg_buffer[128];
    size_t agg_count = 0;

    for (size_t i = 0; i < compiled->num_projections; i++) {
        collect_aggregates(compiled->projections[i], agg_buffer, &agg_count);
    }

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

    // 6. Compile LIMIT and OFFSET
    compiled->limit = sql_compile_expression(ctx, ast->limit);
    compiled->offset = sql_compile_expression(ctx, ast->offset);

    return compiled;
}
