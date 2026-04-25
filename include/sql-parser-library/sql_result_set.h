// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef _sql_result_set_H
#define _sql_result_set_H

#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_node.h"

struct sql_compiled_query_s;

/**
 * A single returned record from the Virtual Machine.
 */
typedef struct {
    sql_node_t **columns;      // The final values for the SELECT projections
    sql_node_t **sort_keys;    // Hidden values used internally to map ORDER BY
    sql_node_t **window_cache; // Hidden cache of partition/sort keys for Window execution
} sql_result_row_t;

/**
 * The materialized output of a Virtual Machine execution.
 * All memory is bound to the parent `sql_ctx_t` pool.
 */
typedef struct {
    aml_pool_t *pool;

    sql_result_row_t *rows;
    size_t count;
    size_t capacity;

    size_t num_columns;
    const char **column_names;
    size_t num_sort_keys;

    int *sort_directions;
    int *sort_projection_indices;

    char *explain_output;      // Populated if query was prefixed with EXPLAIN
    size_t window_cache_size;
} sql_result_set_t;

/** Initializes a new result set payload. */
sql_result_set_t *sql_result_set_init(aml_pool_t *pool, size_t num_columns, const char **column_names, size_t num_sort_keys, int *sort_directions, int *sort_projection_indices);

/** Evaluates the compiled projections against the active VM state and appends the row. */
void sql_result_set_append(sql_ctx_t *ctx, sql_result_set_t *rs, struct sql_compiled_query_s *compiled);

/** Sorts the result set based on the active configuration. */
void sql_result_set_sort(sql_result_set_t *rs);

#endif /* _sql_result_set_H */
