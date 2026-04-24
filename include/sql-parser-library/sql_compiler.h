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

/**
 * Holds execution instructions for a specific window function.
 */
typedef struct {
    size_t projection_index;     // Which SELECT column this window targets
    sql_node_t *func_node;       // The aggregate function to execute
    sql_node_t **partition_exprs;// Keys to group the window by
    size_t num_partition_keys;
    sql_node_t **sort_exprs;     // Keys to order the window by
    int *sort_directions;        // ASC (1) or DESC (-1)
    size_t num_sort_keys;
} sql_window_plan_t;

/**
 * The fully compiled, executable representation of a query.
 * The compiler converts the raw AST into a flat, optimized array of
 * executable sql_node_t trees, extracting aggregates and subqueries.
 */
typedef struct sql_compiled_query_s {
    // Execution: SELECT Phase
    sql_node_t **projections;
    const char **display_names;
    size_t num_projections;

    // Execution: Window Functions
    sql_window_plan_t **window_plans;
    size_t num_window_plans;

    // Execution: GROUP BY Phase
    sql_node_t **group_exprs;
    size_t num_group_keys;

    // Extracted aggregate functions (SUM, COUNT) from all clauses
    sql_node_t **agg_nodes;
    size_t num_aggregates;

    // Execution: HAVING Phase
    sql_node_t *having_filter;

    // Execution: ORDER BY Phase
    sql_node_t **sort_exprs;
    int *sort_directions;
    int *sort_projection_indices; // Links ORDER BY terms to existing SELECT columns
    size_t num_sort_keys;

    // Execution: LIMIT/OFFSET Phase
    sql_node_t *limit;
    sql_node_t *offset;
} sql_compiled_query_t;

/** Compiles a single AST expression into an executable VM node. */
sql_node_t *sql_compile_expression(sql_ctx_t *ctx, struct sql_ast_node_s *ast);

/** Compiles a full parsed query into an executable VM layout. */
sql_compiled_query_t *sql_compile_query(sql_ctx_t *ctx, sql_select_t *ast);

#endif /* _sql_compiler_H */