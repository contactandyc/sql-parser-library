// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef _sql_compiler_H
#define _sql_compiler_H

#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_query.h"
#include "sql-parser-library/sql_planner.h"
#include "sql-parser-library/sql_node.h"

typedef struct {
    size_t projection_index;
    sql_node_t *func_node;
    sql_node_t **partition_exprs;
    size_t num_partition_keys;
    sql_node_t **sort_exprs;
    int *sort_directions;
    size_t num_sort_keys;
} sql_window_plan_t;

typedef struct sql_compiled_query_s {
    sql_node_t **projections;
    const char **display_names;
    size_t num_projections;

    sql_window_plan_t **window_plans;
    size_t num_window_plans;

    sql_node_t **group_exprs;
    size_t num_group_keys;
    sql_node_t **agg_nodes;
    size_t num_aggregates;

    sql_node_t *having_filter;

    sql_node_t **sort_exprs;
    int *sort_directions;
    int *sort_projection_indices; // --- NEW: Links ORDER BY terms to SELECT columns ---
    size_t num_sort_keys;

    sql_node_t *limit;
    sql_node_t *offset;
} sql_compiled_query_t;

sql_node_t *sql_compile_expression(sql_ctx_t *ctx, struct sql_ast_node_s *ast);
sql_compiled_query_t *sql_compile_query(sql_ctx_t *ctx, sql_select_t *ast);

#endif /* _sql_compiler_H */