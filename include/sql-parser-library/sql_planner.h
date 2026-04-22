// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef _sql_planner_H
#define _sql_planner_H

#include "sql-parser-library/sql_query.h"
#include "sql-parser-library/sql_ctx.h"
#include <stdbool.h>

typedef enum {
    SCAN_FULL_TABLE,
    SCAN_INDEX_LOOKUP
} scan_strategy_t;

typedef enum {
    JOIN_ALGO_NESTED_LOOP, // Default fallback
    JOIN_ALGO_HASH_JOIN    // Used when ON clause is an equality match
} sql_join_algo_t;

// A request ticket sent to the storage engine for a single table
typedef struct sql_table_request_s {
    char *table_name;
    char *alias;
    int table_index;

    bool needs_all_columns;
    char **required_columns;
    size_t num_required_columns;

    scan_strategy_t scan_strategy;

    // --- NEW: PREDICATE PUSHDOWN ---
    // Filters that only apply to this specific table.
    // The executor should evaluate these immediately upon reading the row.
    sql_ast_node_t *table_filters;

    struct sql_table_request_s *next;
} sql_table_request_t;

// --- NEW: JOIN PLAN ---
typedef struct sql_join_plan_s {
    sql_join_type_t join_type;    // INNER, LEFT, etc.
    int right_table_index;        // The index of the table being joined IN

    sql_join_algo_t algorithm;    // How the executor should perform the join

    sql_ast_node_t *on_condition; // The AST to evaluate for a match

    struct sql_join_plan_s *next;
} sql_join_plan_t;

// The overall Execution Plan
typedef struct {
    sql_table_request_t *table_requests;

    // --- NEW: JOINS AND GLOBAL FILTERS ---
    sql_join_plan_t *joins;

    // Residual filters that reference MULTIPLE tables (e.g. A.price > B.cost)
    // These must be evaluated AFTER the joins are complete.
    sql_ast_node_t *global_filters;

} sql_execution_plan_t;

sql_execution_plan_t *sql_plan_query(sql_ctx_t *ctx, sql_select_t *ast);
void sql_print_plan(sql_execution_plan_t *plan);
// Optimizes an execution plan by pushing global filters down to individual tables
void sql_pushdown_filters(sql_ctx_t *ctx, sql_execution_plan_t *plan);

#endif /* _sql_planner_H */
