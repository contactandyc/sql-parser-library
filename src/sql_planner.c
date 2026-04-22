// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_planner.h"
#include "a-memory-library/aml_pool.h"
#include <stdio.h>
#include <string.h>
#include <strings.h>

#define MAX_PLAN_TABLES 16
#define MAX_COLS_PER_TABLE 128

// Internal structure used during the AST walk to collect columns safely
typedef struct {
    sql_table_request_t *req;
    const char *temp_cols[MAX_COLS_PER_TABLE];
    size_t temp_count;
} plan_table_state_t;

// Helper: Safely add a column name to a table's temporary list (with deduplication)
static void add_required_column(plan_table_state_t *state, const char *col_name) {
    if (!state || !col_name || state->req->needs_all_columns) return;

    if (state->temp_count >= MAX_COLS_PER_TABLE) return; // Failsafe

    // Deduplicate
    for (size_t i = 0; i < state->temp_count; i++) {
        if (strcasecmp(state->temp_cols[i], col_name) == 0) {
            return; // Already tracked
        }
    }

    state->temp_cols[state->temp_count++] = col_name;
}

// Recursive AST walker to extract physical column names
static void extract_columns_from_ast(sql_ast_node_t *node, plan_table_state_t *states, size_t num_states) {
    if (!node) return;

    // If it's a resolved identifier, we know exactly which table it belongs to!
    if (node->type == SQL_IDENTIFIER && node->column) {
        int idx = node->column->table_index;
        if (idx >= 0 && idx < (int)num_states) {
            // Use the physical column name from the schema, ignoring whatever prefix the user typed
            add_required_column(&states[idx], node->column->name);
        }
    }

    extract_columns_from_ast(node->left, states, num_states);
    extract_columns_from_ast(node->right, states, num_states);
    extract_columns_from_ast(node->next, states, num_states);
}

sql_execution_plan_t *sql_plan_query(sql_ctx_t *ctx, sql_select_t *ast) {
    if (!ast) return NULL;

    sql_execution_plan_t *plan = aml_pool_zalloc(ctx->pool, sizeof(sql_execution_plan_t));

    plan_table_state_t states[MAX_PLAN_TABLES] = {0};
    size_t num_states = 0;

    sql_table_request_t *req_head = NULL;
    sql_table_request_t *req_tail = NULL;

    // 1. Initialize Base Table Request
    if (ast->table) {
        sql_table_request_t *base_req = aml_pool_zalloc(ctx->pool, sizeof(sql_table_request_t));
        base_req->table_name = aml_pool_strdup(ctx->pool, ast->table);
        base_req->alias = ast->table_alias ? aml_pool_strdup(ctx->pool, ast->table_alias) : NULL;
        base_req->table_index = num_states;
        base_req->scan_strategy = SCAN_FULL_TABLE;
        base_req->needs_all_columns = ast->is_star;

        states[num_states].req = base_req;
        num_states++;

        req_head = req_tail = base_req;
    }

    // 2. Initialize Table Requests AND Join Plans for JOINs
    sql_join_plan_t *join_head = NULL;
    sql_join_plan_t *join_tail = NULL;

    sql_join_t *j = ast->joins;
    while (j && num_states < MAX_PLAN_TABLES) {
        // A. Create the Data Request for the table being joined
        sql_table_request_t *join_req = aml_pool_zalloc(ctx->pool, sizeof(sql_table_request_t));
        join_req->table_name = aml_pool_strdup(ctx->pool, j->table);
        join_req->alias = j->alias ? aml_pool_strdup(ctx->pool, j->alias) : NULL;
        join_req->table_index = num_states;
        join_req->scan_strategy = SCAN_FULL_TABLE;
        join_req->needs_all_columns = ast->is_star;

        states[num_states].req = join_req;

        if (req_tail) {
            req_tail->next = join_req;
            req_tail = join_req;
        }

        // B. Create the Join Execution Strategy
        sql_join_plan_t *join_plan = aml_pool_zalloc(ctx->pool, sizeof(sql_join_plan_t));
        join_plan->join_type = j->type;
        join_plan->right_table_index = num_states;
        join_plan->on_condition = j->on_condition; // Attach the ON AST

        // Simple heuristic: If the ON condition is purely an '=' comparison,
        // we can theoretically use a Hash Join. Otherwise, Nested Loop.
        if (j->on_condition && j->on_condition->type == SQL_COMPARISON &&
            strcmp(j->on_condition->value, "=") == 0) {
            join_plan->algorithm = JOIN_ALGO_HASH_JOIN;
        } else {
            join_plan->algorithm = JOIN_ALGO_NESTED_LOOP;
        }

        if (!join_head) join_head = join_tail = join_plan;
        else {
            join_tail->next = join_plan;
            join_tail = join_plan;
        }

        num_states++;
        j = j->next;
    }

    plan->table_requests = req_head;
    plan->joins = join_head;

    // 3. Assign Filters (Phase 1: Everything is Global)
    // Eventually, we will write a function here that recursively fractures `ast->where_clause`
    // and pushes localized nodes down into `states[i].req->table_filters`.
    plan->global_filters = ast->where_clause;


    // 4. Walk the entire AST to harvest required columns
    extract_columns_from_ast(ast->columns, states, num_states);
    extract_columns_from_ast(ast->where_clause, states, num_states);
    extract_columns_from_ast(ast->group_by, states, num_states);
    extract_columns_from_ast(ast->having_clause, states, num_states);
    extract_columns_from_ast(ast->limit, states, num_states);
    extract_columns_from_ast(ast->offset, states, num_states);

    j = ast->joins;
    while (j) {
        extract_columns_from_ast(j->on_condition, states, num_states);
        j = j->next;
    }

    sql_order_by_t *ob = ast->order_by;
    while (ob) {
        extract_columns_from_ast(ob->expr, states, num_states);
        ob = ob->next;
    }

    // 5. Finalize the arrays dynamically from the pool
    for (size_t i = 0; i < num_states; i++) {
        plan_table_state_t *st = &states[i];
        if (st->req->needs_all_columns) continue;

        if (st->temp_count > 0) {
            st->req->num_required_columns = st->temp_count;
            st->req->required_columns = aml_pool_alloc(ctx->pool, st->temp_count * sizeof(char *));
            for (size_t c = 0; c < st->temp_count; c++) {
                st->req->required_columns[c] = aml_pool_strdup(ctx->pool, st->temp_cols[c]);
            }
        }
    }

    return plan;
}

// --- PREDICATE PUSHDOWN LOGIC ---

// Scans an AST node to see which tables it depends on.
// Returns -2 if no tables (constants), -1 if multiple tables, or the table_index if exactly one.
static void check_table_dependency(sql_ast_node_t *node, int *dep_index) {
    if (!node || *dep_index == -1) return;

    if (node->type == SQL_IDENTIFIER && node->column) {
        int idx = node->column->table_index;
        if (*dep_index == -2) {
            *dep_index = idx; // First table seen
        } else if (*dep_index != idx) {
            *dep_index = -1;  // Conflict: Multiple tables referenced!
        }
    }

    check_table_dependency(node->left, dep_index);
    check_table_dependency(node->right, dep_index);
    check_table_dependency(node->next, dep_index);
}

// Appends a filter node to a table's local filter chain
static void append_table_filter(sql_ctx_t *ctx, sql_table_request_t *req, sql_ast_node_t *new_filter) {
    if (!req->table_filters) {
        req->table_filters = new_filter;
    } else {
        // Create a new AND node to glue the existing filters and the new filter together
        sql_ast_node_t *and_node = aml_pool_zalloc(ctx->pool, sizeof(sql_ast_node_t));
        and_node->type = SQL_AND;
        and_node->value = aml_pool_strdup(ctx->pool, "AND");
        and_node->data_type = SQL_TYPE_BOOL;

        // ---> THE MAGIC FIX <---
        and_node->spec = sql_ctx_get_spec(ctx, "AND");

        and_node->left = req->table_filters;
        and_node->right = new_filter;

        req->table_filters = and_node;
    }
}

// Recursively walks the WHERE clause and fractures it along AND boundaries
static sql_ast_node_t *pushdown_node(sql_ctx_t *ctx, sql_execution_plan_t *plan, sql_ast_node_t *node) {
    if (!node) return NULL;

    // If it's an AND, we can attempt to split the left and right sides independently
    if (node->type == SQL_AND) {
        node->left = pushdown_node(ctx, plan, node->left);
        node->right = pushdown_node(ctx, plan, node->right);

        // If both sides were completely pushed down, this AND node is now empty and can be removed
        if (!node->left && !node->right) return NULL;

        // If only one side was pushed down, return the side that is left over
        if (!node->left) return node->right;
        if (!node->right) return node->left;

        // If neither side could be pushed down, return the AND node fully intact
        return node;
    }
    else {
        // It's a solid block (like an OR, a COMPARISON, or an IS NULL).
        // Check its table dependencies.
        int dep = -2;
        check_table_dependency(node, &dep);

        if (dep >= 0) {
            // It only relies on ONE table! Push it down.
            sql_table_request_t *req = plan->table_requests;
            while (req) {
                if (req->table_index == dep) {
                    append_table_filter(ctx, req, node);
                    return NULL; // Return NULL to remove it from the global filters
                }
                req = req->next;
            }
        }

        // It relies on multiple tables (or no tables). Keep it global.
        return node;
    }
}

void sql_pushdown_filters(sql_ctx_t *ctx, sql_execution_plan_t *plan) {
    if (!plan || !plan->global_filters) return;

    // pushdown_node will fracture the tree and return whatever couldn't be pushed down
    plan->global_filters = pushdown_node(ctx, plan, plan->global_filters);
}

void sql_print_plan(sql_execution_plan_t *plan) {
    if (!plan) return;

    printf("\n=== EXECUTION PLAN ===\n");

    printf("--- 1. DATA ACCESS ---\n");
    sql_table_request_t *req = plan->table_requests;
    while (req) {
        printf("TABLE SCAN [Index %d]: %s", req->table_index, req->table_name);
        if (req->alias) printf(" AS %s", req->alias);

        printf("\n  Strategy: %s\n", req->scan_strategy == SCAN_FULL_TABLE ? "Full Table Scan" : "Index Scan");

        printf("  Columns:  ");
        if (req->needs_all_columns) {
            printf("* (All)\n");
        } else if (req->num_required_columns == 0) {
            printf("None (Row Count)\n");
        } else {
            for (size_t i = 0; i < req->num_required_columns; i++) {
                printf("%s%s", req->required_columns[i], i < req->num_required_columns - 1 ? ", " : "");
            }
            printf("\n");
        }

        if (req->table_filters) {
            printf("  Local Filters (Pushed Down):\n");
            print_ast(req->table_filters, 2);
        }

        req = req->next;
        if (req) printf("\n");
    }

    if (plan->joins) {
        printf("\n--- 2. JOINS ---\n");
        sql_join_plan_t *jp = plan->joins;
        while (jp) {
            const char *type_str = jp->join_type == JOIN_LEFT ? "LEFT" :
                                   jp->join_type == JOIN_RIGHT ? "RIGHT" :
                                   jp->join_type == JOIN_FULL ? "FULL OUTER" : "INNER";
            const char *algo_str = jp->algorithm == JOIN_ALGO_HASH_JOIN ? "Hash Join" : "Nested Loop";

            printf("JOIN to [Index %d] (%s via %s)\n", jp->right_table_index, type_str, algo_str);
            jp = jp->next;
        }
    }

    if (plan->global_filters) {
        printf("\n--- 3. GLOBAL FILTERS ---\n");
        printf("Residual filters applied after joins:\n");
        print_ast(plan->global_filters, 1);
    }

    printf("======================\n\n");
}

