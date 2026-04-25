// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef _sql_planner_H
#define _sql_planner_H

#include "sql-parser-library/sql_query.h"
#include "sql-parser-library/sql_ctx.h"
#include "a-memory-library/aml_buffer.h"
#include <stdbool.h>

struct sql_index_s; // Forward declaration

typedef enum {
    SCAN_FULL_TABLE,
    SCAN_INDEX_LOOKUP
} scan_strategy_t;

typedef enum {
    JOIN_ALGO_NESTED_LOOP,
    JOIN_ALGO_HASH_JOIN
} sql_join_algo_t;

/**
 * Represents the execution instructions for retrieving a specific table's data.
 * The Optimizer pushes applicable WHERE clauses directly into this request
 * to act as local filters during the TABLE SCAN phase.
 */
typedef struct sql_table_request_s {
    char *table_name;
    char *alias;
    int table_index;

    bool needs_all_columns;
    char **required_columns;
    size_t num_required_columns;

    scan_strategy_t scan_strategy;
    sql_ast_node_t *table_filters; // Filters that apply *only* to this table

    // Index pushdown instructions
    struct sql_index_s *index_to_use;
    struct sql_node_s **index_exact_values;
    struct sql_node_s **index_min_values;
    struct sql_node_s **index_max_values;
    size_t num_index_values;

    struct sql_table_request_s *next;
} sql_table_request_t;

/**
 * Represents the strategy and conditions for joining two tables.
 */
typedef struct sql_join_plan_s {
    sql_join_type_t join_type;
    int right_table_index;
    sql_join_algo_t algorithm;
    sql_ast_node_t *on_condition;

    struct sql_join_plan_s *next;
} sql_join_plan_t;

/**
 * The unified execution pipeline planned by the query optimizer.
 */
typedef struct {
    sql_table_request_t *table_requests;
    sql_join_plan_t *joins;
    sql_ast_node_t *global_filters; // Residual filters evaluated AFTER joins
} sql_execution_plan_t;

/** Plans the access paths and join algorithms for an AST. */
sql_execution_plan_t *sql_plan_query(sql_ctx_t *ctx, sql_select_t *ast);

/** Attempts to push WHERE filters down into local table requests. */
void sql_pushdown_filters(sql_ctx_t *ctx, sql_execution_plan_t *plan);

/** Formats the execution plan into the provided buffer. */
void sql_print_plan(aml_buffer_t *buf, sql_ctx_t *ctx, sql_execution_plan_t *plan);

#endif /* _sql_planner_H */
