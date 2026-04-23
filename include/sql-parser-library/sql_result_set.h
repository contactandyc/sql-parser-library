// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef _sql_result_set_H
#define _sql_result_set_H

#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_node.h"

// Represents a single materialized row
typedef struct {
    sql_node_t **columns;    // The projected output values
    sql_node_t **sort_keys;  // The evaluated values used purely for ordering
} sql_result_row_t;

// Represents the entire materialized table
typedef struct {
    aml_pool_t *pool;

    sql_result_row_t *rows;
    size_t count;
    size_t capacity;

    size_t num_columns;
    size_t num_sort_keys;

    int *sort_directions; // Array of 1 (ASC) or -1 (DESC)
} sql_result_set_t;

// Creates a new, empty result set
sql_result_set_t *sql_result_set_init(aml_pool_t *pool, size_t num_columns, size_t num_sort_keys, int *sort_directions);

// Evaluates the provided AST projection/sort nodes, deep copies the results, and appends a row
void sql_result_set_append(sql_ctx_t *ctx, sql_result_set_t *rs, sql_node_t **projections, sql_node_t **sort_exprs);

// Sorts the accumulated rows in-place based on the configured sort_directions
void sql_result_set_sort(sql_result_set_t *rs);

#endif /* _sql_result_set_H */
