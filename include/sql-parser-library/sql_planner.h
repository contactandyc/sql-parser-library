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

struct sql_index_s; // Forward declaration

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

    // Filters that only apply to this specific table.
    sql_ast_node_t *table_filters;

    // --- INDEX EXECUTION STATE ---
    struct sql_index_s *index_to_use;
    struct sql_node_s **index_exact_values;
    struct sql_node_s **index_min_values;
    struct sql_node_s **index_max_values;
    size_t num_index_values;

    struct sql_table_request_s *next;
} sql_table_request_t;

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

    sql_join_plan_t *joins;

    // Residual filters that reference MULTIPLE tables (e.g. A.price > B.cost)
    sql_ast_node_t *global_filters;

} sql_execution_plan_t;

sql_execution_plan_t *sql_plan_query(sql_ctx_t *ctx, sql_select_t *ast);

// --- UPDATED: Pass ctx to allow string allocation ---
void sql_print_plan(sql_ctx_t *ctx, sql_execution_plan_t *plan);
void sql_pushdown_filters(sql_ctx_t *ctx, sql_execution_plan_t *plan);

#endif /* _sql_planner_H */
