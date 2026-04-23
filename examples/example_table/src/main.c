// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "a-json-library/ajson.h"
#include "a-memory-library/aml_pool.h"
#include "the-macro-library/macro_map.h"

#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_node.h"
#include "sql-parser-library/sql_ast.h"
#include "sql-parser-library/sql_tokenizer.h"
#include "sql-parser-library/sql_binder.h"
#include "sql-parser-library/sql_planner.h"
#include "sql-parser-library/sql_compiler.h"
#include "sql-parser-library/sql_result_set.h"
#include "sql-parser-library/date_utils.h"

// --- 1. MOCK DATA & CATALOG ---
typedef struct { int id; const char *name; int age; } user_row_t;
typedef struct { int id; int user_id; double total; const char *status; } order_row_t;

user_row_t users[] = {
    {1, "Alice", 25}, {2, "Bob", 30}, {3, "Charlie", 35}, {4, "David", 40}
};
order_row_t orders[] = {
    {101, 1, 250.00, "shipped"}, {102, 1, 45.50, "pending"}, {103, 3, 899.99, "shipped"},
    {104, 4, 12.00, "shipped"}, {105, 1, 950.00, "shipped"}
};

const size_t num_users = sizeof(users) / sizeof(users[0]);
const size_t num_orders = sizeof(orders) / sizeof(orders[0]);

sql_node_t *sql_func_get_data(sql_ctx_t *ctx, sql_node_t *f) {
    void **row_set = (void **)ctx->row;
    if (!row_set || !f->column) return sql_bool_init(ctx, false, true);
    int t_idx = f->column->table_index;
    void *raw_row = row_set[t_idx];
    if (!raw_row) return sql_bool_init(ctx, false, true);
    const char *col_name = f->column->name;

    if (t_idx == 0) {
        user_row_t *u = (user_row_t *)raw_row;
        if (strcasecmp(col_name, "id") == 0) return sql_int_init(ctx, u->id, false);
        if (strcasecmp(col_name, "name") == 0) return sql_string_init(ctx, u->name, false);
        if (strcasecmp(col_name, "age") == 0) return sql_int_init(ctx, u->age, false);
    } else if (t_idx == 1) {
        order_row_t *o = (order_row_t *)raw_row;
        if (strcasecmp(col_name, "id") == 0) return sql_int_init(ctx, o->id, false);
        if (strcasecmp(col_name, "user_id") == 0) return sql_int_init(ctx, o->user_id, false);
        if (strcasecmp(col_name, "total") == 0) return sql_double_init(ctx, o->total, false);
        if (strcasecmp(col_name, "status") == 0) return sql_string_init(ctx, o->status, false);
    }
    return sql_bool_init(ctx, false, true);
}

sql_ctx_column_t *my_dynamic_catalog(sql_ctx_t *ctx, const char *table_name, const char *column_name) {
    if (!table_name) table_name = "users";
    sql_ctx_column_t *col = aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_column_t));
    col->name = aml_pool_strdup(ctx->pool, column_name);
    col->table_name = aml_pool_strdup(ctx->pool, table_name);
    col->func = sql_func_get_data;

    if (strcasecmp(table_name, "users") == 0) {
        if (strcasecmp(column_name, "id") == 0) col->type = SQL_TYPE_INT;
        else if (strcasecmp(column_name, "name") == 0) col->type = SQL_TYPE_STRING;
        else if (strcasecmp(column_name, "age") == 0) col->type = SQL_TYPE_INT;
        else return NULL; return col;
    } else if (strcasecmp(table_name, "orders") == 0) {
        if (strcasecmp(column_name, "id") == 0) col->type = SQL_TYPE_INT;
        else if (strcasecmp(column_name, "user_id") == 0) col->type = SQL_TYPE_INT;
        else if (strcasecmp(column_name, "total") == 0) col->type = SQL_TYPE_DOUBLE;
        else if (strcasecmp(column_name, "status") == 0) col->type = SQL_TYPE_STRING;
        else return NULL; return col;
    }
    return NULL;
}

// --- 2. MACRO MAP GROUPING ENGINE ---

typedef struct {
    macro_map_t link;            // MUST BE FIRST!
    sql_node_t **group_keys;
    size_t num_keys;

    void **agg_states;
    void **first_row_set;        // THE FIX: Snapshot of the raw row pointers
} group_node_t;

// Need the comparator we used in result_sets
static int compare_sql_nodes(sql_node_t *a, sql_node_t *b) {
    if (a->is_null && b->is_null) return 0;
    if (a->is_null) return -1; if (b->is_null) return 1;
    if (a->data_type != b->data_type) return a->data_type - b->data_type;
    switch (a->data_type) {
        case SQL_TYPE_INT: return (a->value.int_value > b->value.int_value) - (a->value.int_value < b->value.int_value);
        case SQL_TYPE_DOUBLE: return (a->value.double_value > b->value.double_value) - (a->value.double_value < b->value.double_value);
        case SQL_TYPE_STRING: return strcmp(a->value.string_value, b->value.string_value);
        default: return 0;
    }
}

static int group_node_cmp(const group_node_t *a, const group_node_t *b) {
    for (size_t i = 0; i < a->num_keys; i++) {
        int cmp = compare_sql_nodes(a->group_keys[i], b->group_keys[i]);
        if (cmp != 0) return cmp;
    }
    return 0;
}

macro_map_insert(group_map_insert, group_node_t, group_node_cmp);
macro_map_find(group_map_find, group_node_t, group_node_cmp);

// Deep copy evaluated nodes for hash map stability
sql_node_t *copy_evaluated_node(sql_ctx_t *ctx, sql_node_t *src) {
    sql_node_t *dst = aml_pool_zalloc(ctx->pool, sizeof(sql_node_t));
    dst->data_type = src->data_type;
    dst->is_null = src->is_null;
    if (src->is_null) return dst;
    if (src->data_type == SQL_TYPE_STRING) {
        dst->value.string_value = aml_pool_strdup(ctx->pool, src->value.string_value);
    } else {
        dst->value = src->value;
    }
    return dst;
}

// --- 3. MAIN PIPELINE ---

int main(int argc, char **argv) {
    const char *query_str = (argc >= 2) ? argv[1] :
        "SELECT u.name AS customer, SUM(o.total) AS lifetime_value, AVG(o.total) as avg_order "
        "FROM users u "
        "JOIN orders o ON u.id = o.user_id "
        "WHERE o.status = 'shipped' "
        "GROUP BY customer "
        "ORDER BY lifetime_value DESC";

    printf("Executing Query:\n%s\n\n", query_str);

    aml_pool_t *pool = aml_pool_init(1024 * 1024 * 10);
    sql_ctx_t context = {0};
    context.pool = pool;
    context.schema_lookup = my_dynamic_catalog;
    register_ctx(&context);

    // 1. Pipeline: Parse -> Bind -> Plan -> Compile
    size_t token_count;
    sql_token_t **tokens = sql_tokenize(&context, query_str, &token_count);
    sql_select_t *query_ast = sql_parse_query(&context, tokens, token_count);
    sql_bind_query_extended(&context, query_ast);
    sql_execution_plan_t *plan = sql_plan_query(&context, query_ast);
    sql_pushdown_filters(&context, plan);

    sql_compiled_query_t *compiled = sql_compile_query(&context, query_ast);

    // Quick local filter pointers
    sql_node_t *users_local_filter = plan->table_requests ? sql_compile_expression(&context, plan->table_requests->table_filters) : NULL;
    sql_node_t *orders_local_filter = plan->table_requests && plan->table_requests->next ? sql_compile_expression(&context, plan->table_requests->next->table_filters) : NULL;
    sql_node_t *join_on_condition = plan->joins ? sql_compile_expression(&context, plan->joins->on_condition) : NULL;
    sql_node_t *global_filter = sql_compile_expression(&context, plan->global_filters);

    sql_result_set_t *rs = sql_result_set_init(pool, compiled->num_projections, compiled->num_sort_keys, compiled->sort_directions);

    // ==========================================
    // PASS 1: ACCUMULATION
    // ==========================================
    macro_map_t *group_map_root = NULL;
    group_node_t *global_group = NULL;

    void *current_row_set[2] = {0};
    context.row = current_row_set;

    for (size_t u = 0; u < num_users; u++) {
        current_row_set[0] = &users[u];

        if (users_local_filter) {
            sql_node_t *res = sql_eval(&context, users_local_filter);
            if (!res || res->is_null || !res->value.bool_value) continue;
        }

        for (size_t o = 0; o < num_orders; o++) {
            current_row_set[1] = &orders[o];

            if (orders_local_filter) {
                sql_node_t *res = sql_eval(&context, orders_local_filter);
                if (!res || res->is_null || !res->value.bool_value) continue;
            }
            if (join_on_condition) {
                sql_node_t *res = sql_eval(&context, join_on_condition);
                if (!res || res->is_null || !res->value.bool_value) continue;
            }
            if (global_filter) {
                sql_node_t *res = sql_eval(&context, global_filter);
                if (!res || res->is_null || !res->value.bool_value) continue;
            }

            group_node_t *active_group = NULL;

            // Route 1: GROUP BY
            if (compiled->num_group_keys > 0) {
                group_node_t lookup_key;
                lookup_key.num_keys = compiled->num_group_keys;
                lookup_key.group_keys = aml_pool_alloc(pool, compiled->num_group_keys * sizeof(sql_node_t *));
                for (size_t k = 0; k < compiled->num_group_keys; k++) {
                    lookup_key.group_keys[k] = sql_eval(&context, compiled->group_exprs[k]);
                }

                active_group = group_map_find(group_map_root, &lookup_key);
                if (!active_group) {
                    active_group = aml_pool_zalloc(pool, sizeof(group_node_t));

                    // --- SNAPSHOT RAW ROW SO NON-AGGREGATE COLUMNS RESOLVE IN PASS 2 ---
                    active_group->first_row_set = aml_pool_alloc(pool, 2 * sizeof(void *));
                    active_group->first_row_set[0] = current_row_set[0];
                    active_group->first_row_set[1] = current_row_set[1];

                    active_group->num_keys = compiled->num_group_keys;
                    active_group->group_keys = aml_pool_alloc(pool, compiled->num_group_keys * sizeof(sql_node_t *));
                    for (size_t k = 0; k < compiled->num_group_keys; k++) {
                        active_group->group_keys[k] = copy_evaluated_node(&context, lookup_key.group_keys[k]);
                    }

                    if (compiled->num_aggregates > 0) {
                        active_group->agg_states = aml_pool_alloc(pool, compiled->num_aggregates * sizeof(void *));
                        for (size_t a = 0; a < compiled->num_aggregates; a++) {
                            active_group->agg_states[a] = aml_pool_zalloc(pool, compiled->agg_nodes[a]->spec->state_size);
                            compiled->agg_nodes[a]->spec->agg_init(active_group->agg_states[a]);
                        }
                    }
                    group_map_insert(&group_map_root, active_group);
                }
            }
            // Route 2: Global Aggregates (No GROUP BY)
            else if (compiled->num_aggregates > 0) {
                if (!global_group) {
                    global_group = aml_pool_zalloc(pool, sizeof(group_node_t));

                    global_group->first_row_set = aml_pool_alloc(pool, 2 * sizeof(void *));
                    global_group->first_row_set[0] = current_row_set[0];
                    global_group->first_row_set[1] = current_row_set[1];

                    global_group->agg_states = aml_pool_alloc(pool, compiled->num_aggregates * sizeof(void *));
                    for (size_t a = 0; a < compiled->num_aggregates; a++) {
                        global_group->agg_states[a] = aml_pool_zalloc(pool, compiled->agg_nodes[a]->spec->state_size);
                        compiled->agg_nodes[a]->spec->agg_init(global_group->agg_states[a]);
                    }
                }
                active_group = global_group;
            }

            // Route 3: Step Aggregates or write directly to Result Set
            if (active_group) {
                for (size_t a = 0; a < compiled->num_aggregates; a++) {
                    compiled->agg_nodes[a]->spec->agg_step(&context, compiled->agg_nodes[a], active_group->agg_states[a]);
                }
            } else {
                sql_result_set_append(&context, rs, compiled->projections, compiled->sort_exprs);
            }
        }
    }

    // ==========================================
    // PASS 2: PROJECTION & MATERIALIZATION
    // ==========================================

    if (compiled->num_group_keys > 0) {
        for (macro_map_t *p = macro_map_first(group_map_root); p; p = macro_map_next(p)) {
            group_node_t *group = (group_node_t *)p;

            context.current_agg_states = group->agg_states;
            context.row = group->first_row_set; // <--- RESTORE THE SNAPSHOT!

            sql_result_set_append(&context, rs, compiled->projections, compiled->sort_exprs);
        }
    } else if (global_group) {
        context.current_agg_states = global_group->agg_states;
        context.row = global_group->first_row_set;

        sql_result_set_append(&context, rs, compiled->projections, compiled->sort_exprs);
    }

    sql_result_set_sort(rs);

    // ==========================================
    // OUTPUT
    // ==========================================
    printf(">> Final Result Set (%zu rows):\n\n", rs->count);
    for (size_t r = 0; r < rs->count; r++) {
        printf("    ");
        for (size_t p = 0; p < compiled->num_projections; p++) {
            printf("%s: ", compiled->display_names[p]);
            sql_node_t *val = rs->rows[r].columns[p];

            if (val->is_null) printf("NULL");
            else switch (val->data_type) {
                case SQL_TYPE_INT:      printf("%d", val->value.int_value); break;
                case SQL_TYPE_DOUBLE:   printf("%.2f", val->value.double_value); break;
                case SQL_TYPE_STRING:   printf("'%s'", val->value.string_value); break;
                default: printf("???");
            }
            if (p < compiled->num_projections - 1) printf(" | ");
        }
        printf("\n");
    }

    printf("\n");
    aml_pool_destroy(pool);
    return 0;
}
