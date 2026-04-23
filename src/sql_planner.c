// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_planner.h"
#include "sql-parser-library/sql_vm.h"
#include "a-memory-library/aml_pool.h"
#include <stdio.h>
#include <string.h>
#include <strings.h>

#define MAX_PLAN_TABLES 16
#define MAX_COLS_PER_TABLE 128

typedef struct {
    sql_table_request_t *req;
    const char *temp_cols[MAX_COLS_PER_TABLE];
    size_t temp_count;
} plan_table_state_t;

static void add_required_column(plan_table_state_t *state, const char *col_name) {
    if (!state || !col_name || state->req->needs_all_columns) return;
    if (state->temp_count >= MAX_COLS_PER_TABLE) return;

    for (size_t i = 0; i < state->temp_count; i++) {
        if (strcasecmp(state->temp_cols[i], col_name) == 0) return;
    }
    state->temp_cols[state->temp_count++] = col_name;
}

static void extract_columns_from_ast(sql_ast_node_t *node, plan_table_state_t *states, size_t num_states) {
    if (!node) return;

    if (node->type == SQL_IDENTIFIER && node->column) {
        int idx = node->column->table_index;
        if (idx >= 0 && idx < (int)num_states) {
            add_required_column(&states[idx], node->column->name);
        }
    }

    // --- FIX: Ensure the planner hunts inside the Window Clause for dependencies! ---
    if (node->window_clause) {
        extract_columns_from_ast(node->window_clause->partition_by, states, num_states);
        sql_order_by_t *ob = node->window_clause->order_by;
        while(ob) {
            extract_columns_from_ast(ob->expr, states, num_states);
            ob = ob->next;
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

    if (ast->table || ast->subquery) {
        sql_table_request_t *base_req = aml_pool_zalloc(ctx->pool, sizeof(sql_table_request_t));

        if (ast->table) {
            base_req->table_name = aml_pool_strdup(ctx->pool, ast->table);
        } else if (ast->table_alias) {
            base_req->table_name = aml_pool_strdup(ctx->pool, ast->table_alias);
        } else {
            base_req->table_name = aml_pool_strdup(ctx->pool, "subquery");
        }

        base_req->alias = ast->table_alias ? aml_pool_strdup(ctx->pool, ast->table_alias) : NULL;
        base_req->table_index = num_states;
        base_req->scan_strategy = SCAN_FULL_TABLE;
        base_req->needs_all_columns = ast->is_star;

        states[num_states].req = base_req;
        num_states++;

        req_head = req_tail = base_req;
    }

    sql_join_plan_t *join_head = NULL;
    sql_join_plan_t *join_tail = NULL;

    sql_join_t *j = ast->joins;
    while (j && num_states < MAX_PLAN_TABLES) {
        sql_table_request_t *join_req = aml_pool_zalloc(ctx->pool, sizeof(sql_table_request_t));

        if (j->table) {
            join_req->table_name = aml_pool_strdup(ctx->pool, j->table);
        } else if (j->alias) {
            join_req->table_name = aml_pool_strdup(ctx->pool, j->alias);
        } else {
            join_req->table_name = aml_pool_strdup(ctx->pool, "subquery");
        }

        join_req->alias = j->alias ? aml_pool_strdup(ctx->pool, j->alias) : NULL;
        join_req->table_index = num_states;
        join_req->scan_strategy = SCAN_FULL_TABLE;
        join_req->needs_all_columns = ast->is_star;

        states[num_states].req = join_req;

        if (req_tail) {
            req_tail->next = join_req;
            req_tail = join_req;
        }

        sql_join_plan_t *join_plan = aml_pool_zalloc(ctx->pool, sizeof(sql_join_plan_t));
        join_plan->join_type = j->type;
        join_plan->right_table_index = num_states;
        join_plan->on_condition = j->on_condition;

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
    plan->global_filters = ast->where_clause;

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

static void check_table_dependency(sql_ast_node_t *node, int *dep_index) {
    if (!node || *dep_index == -1) return;

    if (node->type == SQL_IDENTIFIER && node->column) {
        int idx = node->column->table_index;
        if (*dep_index == -2) {
            *dep_index = idx;
        } else if (*dep_index != idx) {
            *dep_index = -1;
        }
    }

    check_table_dependency(node->left, dep_index);
    check_table_dependency(node->right, dep_index);
    check_table_dependency(node->next, dep_index);
}

static void append_table_filter(sql_ctx_t *ctx, sql_table_request_t *req, sql_ast_node_t *new_filter) {
    if (!req->table_filters) {
        req->table_filters = new_filter;
    } else {
        sql_ast_node_t *and_node = aml_pool_zalloc(ctx->pool, sizeof(sql_ast_node_t));
        and_node->type = SQL_AND;
        and_node->value = aml_pool_strdup(ctx->pool, "AND");
        and_node->data_type = SQL_TYPE_BOOL;

        and_node->spec = sql_ctx_get_spec(ctx, "AND");

        and_node->left = req->table_filters;
        and_node->right = new_filter;

        req->table_filters = and_node;
    }
}

static sql_ast_node_t *pushdown_node(sql_ctx_t *ctx, sql_execution_plan_t *plan, sql_ast_node_t *node, bool *safe_to_pushdown) {
    if (!node) return NULL;

    if (node->type == SQL_AND) {
        node->left = pushdown_node(ctx, plan, node->left, safe_to_pushdown);
        node->right = pushdown_node(ctx, plan, node->right, safe_to_pushdown);

        if (!node->left && !node->right) return NULL;
        if (!node->left) return node->right;
        if (!node->right) return node->left;

        return node;
    }
    else {
        int dep = -2;
        check_table_dependency(node, &dep);

        if (dep >= 0 && safe_to_pushdown[dep]) {
            sql_table_request_t *req = plan->table_requests;
            while (req) {
                if (req->table_index == dep) {
                    append_table_filter(ctx, req, node);
                    return NULL;
                }
                req = req->next;
            }
        }

        return node;
    }
}

void sql_pushdown_filters(sql_ctx_t *ctx, sql_execution_plan_t *plan) {
    if (!plan || !plan->global_filters) return;

    bool safe_to_pushdown[MAX_PLAN_TABLES];
    for (int i = 0; i < MAX_PLAN_TABLES; i++) safe_to_pushdown[i] = true;

    sql_join_plan_t *jp = plan->joins;
    while (jp) {
        if (jp->join_type == JOIN_LEFT || jp->join_type == JOIN_FULL) {
            safe_to_pushdown[jp->right_table_index] = false;
        }
        if (jp->join_type == JOIN_RIGHT || jp->join_type == JOIN_FULL) {
            for (int i = 0; i < jp->right_table_index; i++) {
                safe_to_pushdown[i] = false;
            }
        }
        jp = jp->next;
    }

    plan->global_filters = pushdown_node(ctx, plan, plan->global_filters, safe_to_pushdown);
}

void sql_print_plan(aml_buffer_t *buf, sql_ctx_t *ctx, sql_execution_plan_t *plan) {
    if (!plan) return;

    aml_buffer_appendf(buf, "--- 1. DATA ACCESS ---\n");
    sql_table_request_t *req = plan->table_requests;
    while (req) {
        aml_buffer_appendf(buf, "TABLE SCAN [Index %d]: %s", req->table_index, req->table_name);
        if (req->alias) aml_buffer_appendf(buf, " AS %s", req->alias);

        if (req->scan_strategy == SCAN_INDEX_LOOKUP && req->index_to_use) {
            aml_buffer_appendf(buf, "\n  Strategy: %s Index Lookup on (", req->index_to_use->type == INDEX_TYPE_BTREE ? "B-Tree" : "Hash");
            for(size_t c=0; c < req->num_index_values; c++) {
                aml_buffer_appendf(buf, "%s%s", req->index_to_use->column_names[c], c < req->num_index_values - 1 ? ", " : "");
            }
            aml_buffer_appendf(buf, ")\n");
        } else {
            aml_buffer_appendf(buf, "\n  Strategy: Full Table Scan\n");
        }

        aml_buffer_appendf(buf, "  Columns:  ");
        if (req->needs_all_columns) {
            aml_buffer_appendf(buf, "* (All)\n");
        } else if (req->num_required_columns == 0) {
            aml_buffer_appendf(buf, "None (Row Count)\n");
        } else {
            for (size_t i = 0; i < req->num_required_columns; i++) {
                aml_buffer_appendf(buf, "%s%s", req->required_columns[i], i < req->num_required_columns - 1 ? ", " : "");
            }
            aml_buffer_appendf(buf, "\n");
        }

        if (req->table_filters) {
            char *filter_str = sql_ast_to_string(ctx, req->table_filters);
            aml_buffer_appendf(buf, "  Local Filters (Pushed Down): %s\n", filter_str);
        }

        req = req->next;
        if (req) aml_buffer_appendf(buf, "\n");
    }

    if (plan->joins) {
        aml_buffer_appendf(buf, "--- 2. JOINS ---\n");
        sql_join_plan_t *jp = plan->joins;
        while (jp) {
            const char *type_str = jp->join_type == JOIN_LEFT ? "LEFT" :
                                   jp->join_type == JOIN_RIGHT ? "RIGHT" :
                                   jp->join_type == JOIN_FULL ? "FULL OUTER" : "INNER";
            const char *algo_str = jp->algorithm == JOIN_ALGO_HASH_JOIN ? "Hash Join" : "Nested Loop";

            aml_buffer_appendf(buf, "JOIN to [Index %d] (%s via %s)\n", jp->right_table_index, type_str, algo_str);

            if (jp->on_condition) {
                char *on_str = sql_ast_to_string(ctx, jp->on_condition);
                aml_buffer_appendf(buf, "  Condition: %s\n", on_str);
            }
            jp = jp->next;
        }
        aml_buffer_appendf(buf, "\n");
    }

    if (plan->global_filters) {
        aml_buffer_appendf(buf, "--- 3. GLOBAL FILTERS ---\n");
        char *global_str = sql_ast_to_string(ctx, plan->global_filters);
        aml_buffer_appendf(buf, "Residual filters applied after joins: %s\n", global_str);
    }
}
