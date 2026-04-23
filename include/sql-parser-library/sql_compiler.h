// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0

#ifndef _sql_compiler_H
#define _sql_compiler_H

#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_query.h"
#include "sql-parser-library/sql_planner.h"
#include "sql-parser-library/sql_node.h"

// Holds the fully compiled, executable components for the execution engine
typedef struct {
    // Projections
    sql_node_t **projections;
    const char **display_names;
    size_t num_projections;

    // Aggregations (GROUP BY)
    sql_node_t **group_exprs;
    size_t num_group_keys;
    sql_node_t **agg_nodes;
    size_t num_aggregates;

    // Sorting (ORDER BY)
    sql_node_t **sort_exprs;
    int *sort_directions;
    size_t num_sort_keys;
} sql_compiled_query_t;

sql_node_t *sql_compile_expression(sql_ctx_t *ctx, struct sql_ast_node_s *ast);
sql_compiled_query_t *sql_compile_query(sql_ctx_t *ctx, sql_select_t *ast);

#endif /* _sql_compiler_H */