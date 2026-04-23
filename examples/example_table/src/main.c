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

// --- 1. MOCK DATA ---
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

// --- 2. DATASET ABSTRACTION ---
typedef enum { DS_USERS, DS_ORDERS, DS_SUBQUERY } ds_type_t;

typedef struct {
    ds_type_t type;
    const char *alias;
    size_t count;
    void **rows;
    size_t num_columns;
    const char **column_names;
    sql_data_type_t *column_types;
} dataset_t;

typedef struct {
    dataset_t **datasets;
    size_t num_datasets;
} catalog_state_t;

dataset_t *init_physical_dataset(aml_pool_t *pool, ds_type_t type, const char *alias) {
    dataset_t *ds = aml_pool_zalloc(pool, sizeof(dataset_t));
    ds->type = type;
    ds->alias = alias ? alias : (type == DS_USERS ? "users" : "orders");

    if (type == DS_USERS) {
        ds->count = num_users;
        ds->rows = aml_pool_alloc(pool, num_users * sizeof(void *));
        for(size_t i=0; i < num_users; i++) ds->rows[i] = &users[i];
    } else {
        ds->count = num_orders;
        ds->rows = aml_pool_alloc(pool, num_orders * sizeof(void *));
        for(size_t i=0; i < num_orders; i++) ds->rows[i] = &orders[i];
    }
    return ds;
}

// --- 3. DYNAMIC VIRTUAL CATALOG ---
sql_node_t *copy_evaluated_node(sql_ctx_t *ctx, sql_node_t *src) {
    sql_node_t *dst = aml_pool_zalloc(ctx->pool, sizeof(sql_node_t));
    dst->data_type = src->data_type;
    dst->is_null = src->is_null;

    // THE FIX: Secure operator stability by preserving types
    dst->type = src->type;
    dst->token_type = src->token_type;

    if (src->is_null) return dst;
    if (src->data_type == SQL_TYPE_STRING) {
        dst->value.string_value = aml_pool_strdup(ctx->pool, src->value.string_value);
    } else dst->value = src->value;
    return dst;
}

sql_node_t *sql_func_get_data(sql_ctx_t *ctx, sql_node_t *f) {
    void **row_set = (void **)ctx->row;
    if (!row_set || !f->column) return sql_bool_init(ctx, false, true);

    int t_idx = f->column->table_index;
    void *raw_row = row_set[t_idx];
    if (!raw_row) return sql_bool_init(ctx, false, true);

    catalog_state_t *cs = (catalog_state_t *)ctx->catalog_state;
    dataset_t *ds = cs->datasets[t_idx];
    const char *col_name = f->column->name;

    if (ds->type == DS_USERS) {
        user_row_t *u = (user_row_t *)raw_row;
        if (strcasecmp(col_name, "id") == 0) return sql_int_init(ctx, u->id, false);
        if (strcasecmp(col_name, "name") == 0) return sql_string_init(ctx, u->name, false);
        if (strcasecmp(col_name, "age") == 0) return sql_int_init(ctx, u->age, false);
    }
    else if (ds->type == DS_ORDERS) {
        order_row_t *o = (order_row_t *)raw_row;
        if (strcasecmp(col_name, "id") == 0) return sql_int_init(ctx, o->id, false);
        if (strcasecmp(col_name, "user_id") == 0) return sql_int_init(ctx, o->user_id, false);
        if (strcasecmp(col_name, "total") == 0) return sql_double_init(ctx, o->total, false);
        if (strcasecmp(col_name, "status") == 0) return sql_string_init(ctx, o->status, false);
    }
    else if (ds->type == DS_SUBQUERY) {
        sql_result_row_t *r = (sql_result_row_t *)raw_row;
        for(size_t i=0; i < ds->num_columns; i++) {
            if(!strcasecmp(ds->column_names[i], col_name)) {
                return copy_evaluated_node(ctx, r->columns[i]);
            }
        }
    }
    return sql_bool_init(ctx, false, true);
}

sql_ctx_column_t *my_dynamic_catalog(sql_ctx_t *ctx, const char *table_name, const char *column_name) {
    catalog_state_t *cs = (catalog_state_t *)ctx->catalog_state;
    if (!cs) return NULL;

    for (size_t i = 0; i < cs->num_datasets; i++) {
        dataset_t *ds = cs->datasets[i];
        if (table_name && strcasecmp(ds->alias, table_name) != 0) continue;

        sql_ctx_column_t *col = aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_column_t));
        col->name = aml_pool_strdup(ctx->pool, column_name);
        col->table_name = aml_pool_strdup(ctx->pool, ds->alias);
        col->func = sql_func_get_data;

        if (ds->type == DS_USERS) {
            if (strcasecmp(column_name, "id") == 0) col->type = SQL_TYPE_INT;
            else if (strcasecmp(column_name, "name") == 0) col->type = SQL_TYPE_STRING;
            else if (strcasecmp(column_name, "age") == 0) col->type = SQL_TYPE_INT;
            else continue; return col;
        } else if (ds->type == DS_ORDERS) {
            if (strcasecmp(column_name, "id") == 0) col->type = SQL_TYPE_INT;
            else if (strcasecmp(column_name, "user_id") == 0) col->type = SQL_TYPE_INT;
            else if (strcasecmp(column_name, "total") == 0) col->type = SQL_TYPE_DOUBLE;
            else if (strcasecmp(column_name, "status") == 0) col->type = SQL_TYPE_STRING;
            else continue; return col;
        } else if (ds->type == DS_SUBQUERY) {
            for (size_t c = 0; c < ds->num_columns; c++) {
                if (!strcasecmp(ds->column_names[c], column_name)) {
                    col->type = ds->column_types[c];
                    return col;
                }
            }
        }
    }
    return NULL;
}

// --- 4. MACRO MAP GROUPING ENGINE ---
typedef struct {
    macro_map_t link;
    sql_node_t **group_keys;
    size_t num_keys;
    void **agg_states;
    void **first_row_set;
} group_node_t;

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

// --- 5. GENERIC RECURSIVE LOOP EXECUTOR ---
void execute_nested_loop(sql_ctx_t *ctx, sql_compiled_query_t *compiled,
                         int num_datasets, int current_idx, void **current_row_set,
                         dataset_t **datasets, sql_node_t **local_filters, sql_node_t **join_conds,
                         sql_node_t *global_filter, macro_map_t **group_map_root,
                         group_node_t **global_group, sql_result_set_t *rs) {

    if (current_idx == num_datasets) {
        if (global_filter) {
            sql_node_t *res = sql_eval(ctx, global_filter);
            if (!res || res->is_null || !res->value.bool_value) return;
        }

        group_node_t *active_group = NULL;

        if (compiled->num_group_keys > 0) {
            group_node_t lookup_key;
            lookup_key.num_keys = compiled->num_group_keys;
            lookup_key.group_keys = aml_pool_alloc(ctx->pool, compiled->num_group_keys * sizeof(sql_node_t *));
            for (size_t k = 0; k < compiled->num_group_keys; k++) {
                lookup_key.group_keys[k] = sql_eval(ctx, compiled->group_exprs[k]);
            }

            active_group = group_map_find(*group_map_root, &lookup_key);
            if (!active_group) {
                active_group = aml_pool_zalloc(ctx->pool, sizeof(group_node_t));

                active_group->first_row_set = aml_pool_alloc(ctx->pool, num_datasets * sizeof(void *));
                for (int c = 0; c < num_datasets; c++) active_group->first_row_set[c] = current_row_set[c];

                active_group->num_keys = compiled->num_group_keys;
                active_group->group_keys = aml_pool_alloc(ctx->pool, compiled->num_group_keys * sizeof(sql_node_t *));
                for (size_t k = 0; k < compiled->num_group_keys; k++) {
                    active_group->group_keys[k] = copy_evaluated_node(ctx, lookup_key.group_keys[k]);
                }

                if (compiled->num_aggregates > 0) {
                    active_group->agg_states = aml_pool_alloc(ctx->pool, compiled->num_aggregates * sizeof(void *));
                    for (size_t a = 0; a < compiled->num_aggregates; a++) {
                        active_group->agg_states[a] = aml_pool_zalloc(ctx->pool, compiled->agg_nodes[a]->spec->state_size);
                        compiled->agg_nodes[a]->spec->agg_init(active_group->agg_states[a]);
                    }
                }
                group_map_insert(group_map_root, active_group);
            }
        }
        else if (compiled->num_aggregates > 0) {
            if (!*global_group) {
                *global_group = aml_pool_zalloc(ctx->pool, sizeof(group_node_t));

                (*global_group)->first_row_set = aml_pool_alloc(ctx->pool, num_datasets * sizeof(void *));
                for (int c = 0; c < num_datasets; c++) (*global_group)->first_row_set[c] = current_row_set[c];

                (*global_group)->agg_states = aml_pool_alloc(ctx->pool, compiled->num_aggregates * sizeof(void *));
                for (size_t a = 0; a < compiled->num_aggregates; a++) {
                    (*global_group)->agg_states[a] = aml_pool_zalloc(ctx->pool, compiled->agg_nodes[a]->spec->state_size);
                    compiled->agg_nodes[a]->spec->agg_init((*global_group)->agg_states[a]);
                }
            }
            active_group = *global_group;
        }

        if (active_group) {
            for (size_t a = 0; a < compiled->num_aggregates; a++) {
                compiled->agg_nodes[a]->spec->agg_step(ctx, compiled->agg_nodes[a], active_group->agg_states[a]);
            }
        } else {
            sql_result_set_append(ctx, rs, compiled->projections, compiled->sort_exprs);
        }
        return;
    }

    dataset_t *ds = datasets[current_idx];
    sql_node_t *local_filter = local_filters[current_idx];
    sql_node_t *join_cond = join_conds[current_idx];

    for (size_t i = 0; i < ds->count; i++) {
        current_row_set[current_idx] = ds->rows[i];

        if (local_filter) {
            sql_node_t *res = sql_eval(ctx, local_filter);
            if (!res || res->is_null || !res->value.bool_value) continue;
        }
        if (join_cond) {
            sql_node_t *res = sql_eval(ctx, join_cond);
            if (!res || res->is_null || !res->value.bool_value) continue;
        }

        execute_nested_loop(ctx, compiled, num_datasets, current_idx + 1, current_row_set,
                            datasets, local_filters, join_conds, global_filter,
                            group_map_root, global_group, rs);
    }
}

// --- 6. THE VIRTUAL MACHINE PIPELINE ---
dataset_t *execute_query(sql_ctx_t *ctx, sql_select_t *ast, const char *forced_alias) {
    if (!ast) return NULL;

    dataset_t **datasets = aml_pool_alloc(ctx->pool, 16 * sizeof(dataset_t *));
    size_t num_datasets = 0;

    if (ast->subquery) {
        datasets[num_datasets++] = execute_query(ctx, ast->subquery, ast->table_alias ? ast->table_alias : "subquery");
    } else if (ast->table) {
        ds_type_t t = strcasecmp(ast->table, "users") == 0 ? DS_USERS : DS_ORDERS;
        datasets[num_datasets++] = init_physical_dataset(ctx->pool, t, ast->table_alias ? ast->table_alias : ast->table);
    }

    sql_join_t *j = ast->joins;
    while (j) {
        if (j->subquery) {
            datasets[num_datasets++] = execute_query(ctx, j->subquery, j->alias ? j->alias : "subquery");
        } else if (j->table) {
            ds_type_t t = strcasecmp(j->table, "users") == 0 ? DS_USERS : DS_ORDERS;
            datasets[num_datasets++] = init_physical_dataset(ctx->pool, t, j->alias ? j->alias : j->table);
        }
        j = j->next;
    }

    catalog_state_t *cs = aml_pool_alloc(ctx->pool, sizeof(catalog_state_t));
    cs->datasets = datasets;
    cs->num_datasets = num_datasets;

    void *old_catalog_state = ctx->catalog_state;
    void *old_row = ctx->row;
    void **old_agg_states = ctx->current_agg_states;

    ctx->catalog_state = cs;

    sql_bind_query_extended(ctx, ast);
    sql_execution_plan_t *plan = sql_plan_query(ctx, ast);
    sql_pushdown_filters(ctx, plan);
    sql_compiled_query_t *compiled = sql_compile_query(ctx, ast);

    sql_node_t **local_filters = aml_pool_zalloc(ctx->pool, num_datasets * sizeof(sql_node_t *));
    sql_table_request_t *req = plan->table_requests;
    while(req) {
        local_filters[req->table_index] = sql_compile_expression(ctx, req->table_filters);
        req = req->next;
    }

    sql_node_t **join_conds = aml_pool_zalloc(ctx->pool, num_datasets * sizeof(sql_node_t *));
    sql_join_plan_t *jp = plan->joins;
    while(jp) {
        join_conds[jp->right_table_index] = sql_compile_expression(ctx, jp->on_condition);
        jp = jp->next;
    }

    sql_node_t *global_filter = sql_compile_expression(ctx, plan->global_filters);

    sql_result_set_t *rs = sql_result_set_init(ctx->pool, compiled->num_projections, compiled->num_sort_keys, compiled->sort_directions);
    macro_map_t *group_map_root = NULL;
    group_node_t *global_group = NULL;

    void **current_row_set = aml_pool_zalloc(ctx->pool, num_datasets * sizeof(void *));
    ctx->row = current_row_set;

    execute_nested_loop(ctx, compiled, num_datasets, 0, current_row_set,
                        datasets, local_filters, join_conds, global_filter,
                        &group_map_root, &global_group, rs);

    if (compiled->num_group_keys > 0) {
        for (macro_map_t *p = macro_map_first(group_map_root); p; p = macro_map_next(p)) {
            group_node_t *group = (group_node_t *)p;
            ctx->current_agg_states = group->agg_states;
            ctx->row = group->first_row_set;

            if (compiled->having_filter) {
                sql_node_t *res = sql_eval(ctx, compiled->having_filter);
                if (!res || res->is_null || !res->value.bool_value) continue;
            }
            sql_result_set_append(ctx, rs, compiled->projections, compiled->sort_exprs);
        }
    } else if (global_group) {
        ctx->current_agg_states = global_group->agg_states;
        ctx->row = global_group->first_row_set;

        bool pass = true;
        if (compiled->having_filter) {
            sql_node_t *res = sql_eval(ctx, compiled->having_filter);
            if (!res || res->is_null || !res->value.bool_value) pass = false;
        }
        if (pass) sql_result_set_append(ctx, rs, compiled->projections, compiled->sort_exprs);
    }

    sql_result_set_sort(rs);

    ctx->catalog_state = old_catalog_state;
    ctx->row = old_row;
    ctx->current_agg_states = old_agg_states;

    dataset_t *out_ds = aml_pool_zalloc(ctx->pool, sizeof(dataset_t));
    out_ds->type = DS_SUBQUERY;
    out_ds->alias = forced_alias;
    out_ds->count = rs->count;
    out_ds->rows = aml_pool_alloc(ctx->pool, rs->count * sizeof(void *));
    for(size_t i=0; i < rs->count; i++) out_ds->rows[i] = &rs->rows[i];

    out_ds->num_columns = compiled->num_projections;
    out_ds->column_names = compiled->display_names;
    out_ds->column_types = aml_pool_alloc(ctx->pool, out_ds->num_columns * sizeof(sql_data_type_t));
    for (size_t c = 0; c < out_ds->num_columns; c++) {
        out_ds->column_types[c] = compiled->projections[c]->data_type;
    }
    return out_ds;
}

// --- 7. MAIN DEMO ---
int main(int argc, char **argv) {
    const char *query_str = (argc >= 2) ? argv[1] :
        "SELECT u.name AS customer, o.total "
        "FROM (SELECT id, name FROM users) u "
        "JOIN (SELECT user_id, SUM(total) AS total FROM orders GROUP BY user_id) o ON u.id = o.user_id "
        "ORDER BY o.total DESC";

    printf("Executing Query:\n%s\n\n", query_str);

    aml_pool_t *pool = aml_pool_init(1024 * 1024 * 10);
    sql_ctx_t context = {0};
    context.pool = pool;
    context.schema_lookup = my_dynamic_catalog;
    register_ctx(&context);

    size_t token_count;
    sql_token_t **tokens = sql_tokenize(&context, query_str, &token_count);
    sql_select_t *query_ast = sql_parse_query(&context, tokens, token_count);

    dataset_t *final_results = execute_query(&context, query_ast, "final_results");

    printf(">> Final Result Set (%zu rows):\n\n", final_results->count);
    for (size_t r = 0; r < final_results->count; r++) {
        sql_result_row_t *row = (sql_result_row_t *)final_results->rows[r];
        printf("    ");
        for (size_t p = 0; p < final_results->num_columns; p++) {
            printf("%s: ", final_results->column_names[p]);
            sql_node_t *val = row->columns[p];

            if (val->is_null) printf("NULL");
            else switch (val->data_type) {
                case SQL_TYPE_INT:      printf("%d", val->value.int_value); break;
                case SQL_TYPE_DOUBLE:   printf("%.2f", val->value.double_value); break;
                case SQL_TYPE_STRING:   printf("'%s'", val->value.string_value); break;
                default: printf("???");
            }
            if (p < final_results->num_columns - 1) printf(" | ");
        }
        printf("\n");
    }

    printf("\n");
    aml_pool_destroy(pool);
    return 0;
}