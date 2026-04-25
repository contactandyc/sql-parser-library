// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_vm.h"
#include "sql-parser-library/sql_compiler.h"
#include "sql-parser-library/sql_binder.h"
#include "the-macro-library/macro_map.h"
#include "the-macro-library/macro_sort.h"
#include "a-memory-library/aml_buffer.h"
#include <string.h>
#include <stdio.h>

#define ENABLE_VM_DEBUG
#ifdef ENABLE_VM_DEBUG
    #define VM_DEBUG(...) printf(__VA_ARGS__)
#else
    #define VM_DEBUG(...) do {} while (0)
#endif

typedef struct {
    sql_vm_t *vm;
    sql_dataset_t **datasets;
    size_t num_datasets;
} vm_catalog_state_t;

static sql_dataset_t *internal_execute(sql_vm_t *vm, sql_select_t *ast, const char *forced_alias, sql_dataset_t **parent_ctes, size_t num_parent_ctes, bool explain_mode);

static sql_node_t *copy_evaluated_node(sql_ctx_t *ctx, sql_node_t *src) {
    sql_node_t *dst = aml_pool_zalloc(ctx->pool, sizeof(sql_node_t));
    dst->data_type = src->data_type;
    dst->is_null = src->is_null;
    dst->type = src->type;
    dst->token_type = src->token_type;
    if (src->is_null) return dst;
    if (src->data_type == SQL_TYPE_STRING) {
        dst->value.string_value = aml_pool_strdup(ctx->pool, src->value.string_value);
    } else dst->value = src->value;
    return dst;
}

static sql_node_t *vm_virtual_func_get_data(sql_ctx_t *ctx, sql_node_t *f) {
    sql_ctx_t *target_ctx = ctx;
    for (int i = 0; i < f->column->scope_depth; i++) target_ctx = target_ctx->parent_ctx;

    void **row_set = (void **)target_ctx->row;
    if (!row_set || !f->column) return sql_bool_init(ctx, false, true);

    int t_idx = f->column->table_index;
    void *raw_row = row_set[t_idx];
    if (!raw_row) return sql_bool_init(ctx, false, true);

    vm_catalog_state_t *cs = (vm_catalog_state_t *)target_ctx->catalog_state;
    sql_dataset_t *ds = cs->datasets[t_idx];
    const char *col_name = f->column->name;

    sql_result_row_t *r = (sql_result_row_t *)raw_row;
    for(size_t i=0; i < ds->num_columns; i++) {
        if(!strcasecmp(ds->column_names[i], col_name)) {
            return copy_evaluated_node(ctx, r->columns[i]);
        }
    }
    return sql_bool_init(ctx, false, true);
}

static sql_ctx_column_t *vm_catalog_router(sql_ctx_t *ctx, const char *table_name, const char *column_name) {
    vm_catalog_state_t *cs = (vm_catalog_state_t *)ctx->catalog_state;
    if (!cs) return NULL;
    sql_vm_t *vm = cs->vm;

    for (size_t i = 0; i < cs->num_datasets; i++) {
        sql_dataset_t *ds = cs->datasets[i];

        if (table_name && strcasecmp(ds->alias, table_name) != 0 &&
           (!ds->table_name || strcasecmp(ds->table_name, table_name) != 0)) {
            continue;
        }

        if (ds->is_virtual) {
            for (size_t c = 0; c < ds->num_columns; c++) {
                if (!strcasecmp(ds->column_names[c], column_name)) {
                    sql_ctx_column_t *col = aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_column_t));
                    col->name = aml_pool_strdup(ctx->pool, column_name);
                    col->table_name = aml_pool_strdup(ctx->pool, ds->alias);
                    col->func = vm_virtual_func_get_data;
                    col->type = ds->column_types[c];
                    col->scope_depth = 0; // Local Scope
                    return col;
                }
            }
        } else {
            const char *physical_name = ds->table_name ? ds->table_name : ds->alias;
            sql_ctx_column_t *col = vm->resolve_column(vm, physical_name, column_name);
            if (col) {
                col->table_name = aml_pool_strdup(ctx->pool, ds->alias);
                col->scope_depth = 0; // Local Scope
                return col;
            }
        }
    }

    // Search outer scopes!
    if (ctx->parent_ctx) {
        sql_ctx_column_t *parent_col = ctx->parent_ctx->schema_lookup(ctx->parent_ctx, table_name, column_name);
        if (parent_col) {
            parent_col->scope_depth += 1;
            return parent_col;
        }
    }

    return NULL;
}

sql_node_t *sql_vm_eval_subquery(sql_ctx_t *ctx, sql_node_t *f) {
    aml_pool_t *reusable_pool = (aml_pool_t *)f->value.custom;
    aml_pool_clear(reusable_pool);

    sql_ctx_t inner_ctx = *ctx;
    inner_ctx.pool = reusable_pool;
    inner_ctx.parent_ctx = ctx;
    inner_ctx.row = NULL;
    inner_ctx.current_agg_states = NULL;

    // --- NEW: Isolate the GC list! ---
    inner_ctx.tracked_pools = NULL;

    sql_vm_t inner_vm = *(ctx->vm);
    inner_vm.ctx = &inner_ctx;
    inner_vm.pool = reusable_pool;

    // Call the PUBLIC wrapper. It will execute and then clean up its own internal tracked_pools!
    sql_result_set_t *rs = sql_vm_execute(&inner_vm, f->subquery_ast);

    sql_node_t *result = NULL;
    if (f->type == SQL_EXISTS) {
        bool exists = (rs && rs->count > 0);
        result = sql_bool_init(ctx, exists, false);
    } else if (f->type == SQL_NODE_SUBQUERY) {
        if (rs && rs->count == 1 && rs->num_columns == 1) {
            result = copy_evaluated_node(ctx, rs->rows[0].columns[0]);
        }
    }

    if (!result) result = sql_bool_init(ctx, false, true);

    return result;
}

static void manual_bind_ast(sql_ctx_t *ctx, sql_ast_node_t *node, vm_catalog_state_t *cs) {
    if (!node) return;
    if (node->type == SQL_IDENTIFIER && !node->column) {
        char *tbl = NULL;
        char *col = node->value;
        char *dot = strchr(node->value, '.');
        if (dot) {
            tbl = aml_pool_strndup(ctx->pool, node->value, dot - node->value);
            col = dot + 1;
        }
        node->column = vm_catalog_router(ctx, tbl, col);
        if (node->column) {
            node->column->table_index = -1;
            for (size_t i = 0; i < cs->num_datasets; i++) {
                if (strcasecmp(cs->datasets[i]->alias, node->column->table_name) == 0) {
                    node->column->table_index = (int)i;
                    break;
                }
            }
        } else {
            sql_ctx_error(ctx, "Unknown column in window clause: %s", node->value);
        }
    }
    manual_bind_ast(ctx, node->left, cs);
    manual_bind_ast(ctx, node->right, cs);
}

static void bind_window_expressions(sql_ctx_t *ctx, sql_select_t *ast, vm_catalog_state_t *cs) {
    sql_ast_node_t *col = ast->columns;
    while (col) {
        if (col->window_clause) {
            sql_ast_node_t *p = col->window_clause->partition_by;
            while (p) {
                manual_bind_ast(ctx, p, cs);
                p = p->next;
            }
            sql_order_by_t *ob = col->window_clause->order_by;
            while (ob) {
                manual_bind_ast(ctx, ob->expr, cs);
                ob = ob->next;
            }
        }
        col = col->next;
    }
}


sql_vm_t *sql_vm_init(sql_ctx_t *ctx, sql_vm_fetch_table_cb fetch_cb, sql_vm_resolve_column_cb resolve_cb, void *user_data) {
    sql_vm_t *vm = aml_pool_zalloc(ctx->pool, sizeof(sql_vm_t));
    vm->ctx = ctx;
    vm->pool = ctx->pool;
    vm->fetch_table = fetch_cb;
    vm->resolve_column = resolve_cb;
    vm->user_data = user_data;

    ctx->schema_lookup = vm_catalog_router;
    ctx->vm = vm;

    return vm;
}

sql_dataset_t *sql_vm_create_materialized_dataset(sql_vm_t *vm, size_t count, void **rows) {
    sql_dataset_t *ds = aml_pool_zalloc(vm->pool, sizeof(sql_dataset_t));
    ds->is_virtual = false;
    ds->mode = DS_MODE_MATERIALIZED;
    ds->count = count;
    ds->rows = rows;
    return ds;
}

sql_dataset_t *sql_vm_create_streaming_dataset(sql_vm_t *vm, void *stream_state,
                                               bool (*next_cb)(sql_dataset_t*, void**),
                                               void (*rewind_cb)(sql_dataset_t*),
                                               void (*close_cb)(sql_dataset_t*)) {
    sql_dataset_t *ds = aml_pool_zalloc(vm->pool, sizeof(sql_dataset_t));
    ds->is_virtual = false;
    ds->mode = DS_MODE_STREAMING;
    ds->stream_state = stream_state;
    ds->next = next_cb;
    ds->rewind = rewind_cb;
    ds->close = close_cb;
    return ds;
}

typedef struct join_row_s {
    void *row;
    struct join_row_s *next;
} join_row_t;

typedef struct {
    macro_map_t link;
    sql_node_t *key;
    join_row_t *rows;

    size_t num_keys;
    sql_node_t **group_keys;
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
    if (a->key && b->key) return compare_sql_nodes(a->key, b->key);
    for (size_t i = 0; i < a->num_keys; i++) {
        int cmp = compare_sql_nodes(a->group_keys[i], b->group_keys[i]);
        if (cmp != 0) return cmp;
    }
    return 0;
}

static inline
macro_map_insert(group_map_insert, group_node_t, group_node_cmp);

static inline
macro_map_find(group_map_find, group_node_t, group_node_cmp);

static void check_node_dep(sql_node_t *node, int *dep) {
    if (!node || *dep == -1) return;

    if (node->column) {
        int idx = node->column->table_index;
        if (*dep == -2) *dep = idx;
        else if (*dep != idx) *dep = -1;
    }
    for (size_t i = 0; i < node->num_parameters; i++) {
        check_node_dep(node->parameters[i], dep);
    }
}

static bool node_depends_on_table(sql_node_t *node, int t_idx) {
    if (!node) return false;
    if (node->column && node->column->table_index == t_idx) return true;
    for (size_t i = 0; i < node->num_parameters; i++) {
        if (node_depends_on_table(node->parameters[i], t_idx)) return true;
    }
    return false;
}

static void harvest_index_conditions(sql_ctx_t *ctx, sql_node_t *filter_node, int table_idx, const char *col_name,
                                     sql_node_t **out_exact, sql_node_t **out_min, sql_node_t **out_max) {
    if (!filter_node) return;

    if (filter_node->token_type == SQL_AND || filter_node->type == SQL_AND) {
        for (size_t i = 0; i < filter_node->num_parameters; i++) {
            harvest_index_conditions(ctx, filter_node->parameters[i], table_idx, col_name, out_exact, out_min, out_max);
        }
        return;
    }

    if (filter_node->type == SQL_COMPARISON && filter_node->num_parameters == 2) {
        sql_node_t *left = filter_node->parameters[0];
        sql_node_t *right = filter_node->parameters[1];

        sql_node_t *col_node = NULL, *val_node = NULL;
        bool is_col_left = false;

        if (left->column && left->column->table_index == table_idx && strcasecmp(left->column->name, col_name) == 0) {
            col_node = left; val_node = right; is_col_left = true;
        } else if (right->column && right->column->table_index == table_idx && strcasecmp(right->column->name, col_name) == 0) {
            col_node = right; val_node = left; is_col_left = false;
        }

        if (col_node && val_node) {
            int dep = -2; check_node_dep(val_node, &dep);
            if (dep != table_idx) {
                if (strcmp(filter_node->token, "=") == 0) {
                    *out_exact = val_node;
                } else if (strcmp(filter_node->token, ">") == 0 || strcmp(filter_node->token, ">=") == 0) {
                    if (is_col_left) *out_min = val_node; else *out_max = val_node;
                } else if (strcmp(filter_node->token, "<") == 0 || strcmp(filter_node->token, "<=") == 0) {
                    if (is_col_left) *out_max = val_node; else *out_min = val_node;
                }
            }
        }
    }
}

static void optimize_indexes_for_table(sql_ctx_t *ctx, sql_table_request_t *req, sql_dataset_t *ds, sql_node_t *local_filter) {
    if (!ds->indexes || ds->num_indexes == 0 || !local_filter) return;

    int best_index = -1;
    size_t best_score = 0;
    sql_node_t *best_exact[16] = {0}, *best_min[16] = {0}, *best_max[16] = {0};

    for (size_t i = 0; i < ds->num_indexes; i++) {
        sql_index_t *idx = ds->indexes[i];
        size_t score = 0;
        sql_node_t *temp_exact[16] = {0}, *temp_min[16] = {0}, *temp_max[16] = {0};

        for (size_t c = 0; c < idx->num_columns && c < 16; c++) {
            harvest_index_conditions(ctx, local_filter, req->table_index, idx->column_names[c],
                                     &temp_exact[c], &temp_min[c], &temp_max[c]);

            if (temp_exact[c]) {
                score += 10;
            } else if (temp_min[c] || temp_max[c]) {
                if (idx->type == INDEX_TYPE_BTREE) score += 5;
                break;
            } else {
                break;
            }
        }

        if (score > best_score) {
            best_score = score;
            best_index = (int)i;
            memcpy(best_exact, temp_exact, sizeof(best_exact));
            memcpy(best_min, temp_min, sizeof(best_min));
            memcpy(best_max, temp_max, sizeof(best_max));
        }
    }

    if (best_index >= 0) {
        sql_index_t *idx = ds->indexes[best_index];
        req->scan_strategy = SCAN_INDEX_LOOKUP;
        req->index_to_use = idx;

        size_t matched_cols = 0;
        while(matched_cols < idx->num_columns && (best_exact[matched_cols] || best_min[matched_cols] || best_max[matched_cols])) matched_cols++;

        req->num_index_values = matched_cols;
        req->index_exact_values = aml_pool_alloc(ctx->pool, matched_cols * sizeof(sql_node_t *));
        req->index_min_values = aml_pool_alloc(ctx->pool, matched_cols * sizeof(sql_node_t *));
        req->index_max_values = aml_pool_alloc(ctx->pool, matched_cols * sizeof(sql_node_t *));

        memcpy(req->index_exact_values, best_exact, matched_cols * sizeof(sql_node_t *));
        memcpy(req->index_min_values, best_min, matched_cols * sizeof(sql_node_t *));
        memcpy(req->index_max_values, best_max, matched_cols * sizeof(sql_node_t *));
    }
}

static void execute_nested_loop(sql_ctx_t *ctx, sql_compiled_query_t *compiled,
                         int num_datasets, int step, int *exec_order, void **current_row_set,
                         sql_dataset_t **datasets, sql_table_request_t **req_array,
                         sql_node_t **local_filters, sql_node_t **step_join_conds,
                         sql_join_type_t *step_join_types,
                         sql_node_t *global_filter, macro_map_t **group_map_root,
                         group_node_t **global_group, sql_result_set_t *rs,
                         macro_map_t **join_maps, sql_node_t **join_probe_exprs) {

    if (step == num_datasets) {
        if (global_filter) {
            sql_node_t *res = sql_eval(ctx, global_filter);
            if (!res || res->is_null || !res->value.bool_value) return;
        }

        group_node_t *active_group = NULL;

        if (compiled->num_group_keys > 0) {
            group_node_t lookup_key = {0};
            lookup_key.num_keys = compiled->num_group_keys;
            lookup_key.group_keys = aml_pool_alloc(ctx->pool, compiled->num_group_keys * sizeof(sql_node_t *));
            for (size_t k = 0; k < compiled->num_group_keys; k++) {
                lookup_key.group_keys[k] = sql_eval(ctx, compiled->group_exprs[k]);
            }

            active_group = group_map_find(*group_map_root, &lookup_key);
            if (!active_group) {
                active_group = aml_pool_zalloc(ctx->pool, sizeof(group_node_t));
                active_group->first_row_set = aml_pool_alloc(ctx->pool, num_datasets * sizeof(void *));

                // --- CLONE ROW FIX FOR GROUP BUCKETS ---
                for (int c = 0; c < num_datasets; c++) {
                    if (datasets[c]->clone_row && current_row_set[c]) {
                        active_group->first_row_set[c] = datasets[c]->clone_row(datasets[c], current_row_set[c], ctx->pool);
                    } else {
                        active_group->first_row_set[c] = current_row_set[c];
                    }
                }

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
        } else if (compiled->num_aggregates > 0) {
            if (!*global_group) {
                *global_group = aml_pool_zalloc(ctx->pool, sizeof(group_node_t));
                (*global_group)->first_row_set = aml_pool_alloc(ctx->pool, num_datasets * sizeof(void *));

                // --- CLONE ROW FIX FOR GLOBAL GROUP ---
                for (int c = 0; c < num_datasets; c++) {
                    if (datasets[c]->clone_row && current_row_set[c]) {
                        (*global_group)->first_row_set[c] = datasets[c]->clone_row(datasets[c], current_row_set[c], ctx->pool);
                    } else {
                        (*global_group)->first_row_set[c] = current_row_set[c];
                    }
                }

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
            sql_result_set_append(ctx, rs, compiled);
        }
        return;
    }

    int t_idx = exec_order[step];
    sql_table_request_t *req = req_array[t_idx];

    if (join_maps && join_maps[step]) {
        sql_node_t *probe_key = sql_eval(ctx, join_probe_exprs[step]);
        bool matched_any = false;

        if (probe_key && !probe_key->is_null) {
            group_node_t lookup = {0};
            lookup.key = probe_key;
            group_node_t *found = group_map_find(join_maps[step], &lookup);

            if (found) {
                join_row_t *jr = found->rows;
                while (jr) {
                    current_row_set[t_idx] = jr->row;
                    matched_any = true;
                    execute_nested_loop(ctx, compiled, num_datasets, step + 1, exec_order, current_row_set,
                                        datasets, req_array, local_filters, step_join_conds, step_join_types, global_filter,
                                        group_map_root, global_group, rs, join_maps, join_probe_exprs);
                    jr = jr->next;
                }
            }
        }

        if (!matched_any && step_join_types[step] == JOIN_LEFT) {
            current_row_set[t_idx] = NULL;
            execute_nested_loop(ctx, compiled, num_datasets, step + 1, exec_order, current_row_set,
                                datasets, req_array, local_filters, step_join_conds, step_join_types, global_filter,
                                group_map_root, global_group, rs, join_maps, join_probe_exprs);
        }

        return;
    }

    sql_dataset_t *ds = datasets[t_idx];
    sql_node_t *local_filter = local_filters[t_idx];
    sql_node_t *join_cond = step_join_conds[step];
    bool matched_any = false;

    if (req && req->scan_strategy == SCAN_INDEX_LOOKUP && req->index_to_use) {
        size_t match_count = 0;
        void **matched_rows = NULL;

        sql_node_t *eval_exacts[16] = {0};
        sql_node_t *eval_mins[16] = {0};
        sql_node_t *eval_maxs[16] = {0};

        for(size_t c=0; c < req->num_index_values; c++) {
            if(req->index_exact_values[c]) eval_exacts[c] = sql_eval(ctx, req->index_exact_values[c]);
            if(req->index_min_values[c]) eval_mins[c] = sql_eval(ctx, req->index_min_values[c]);
            if(req->index_max_values[c]) eval_maxs[c] = sql_eval(ctx, req->index_max_values[c]);
        }

        bool is_range = false;
        for(size_t c=0; c < req->num_index_values; c++) {
            if (eval_mins[c] || eval_maxs[c]) is_range = true;
        }

        if (is_range && req->index_to_use->lookup_range) {
            matched_rows = req->index_to_use->lookup_range(req->index_to_use->index_state, eval_mins, eval_maxs, req->num_index_values, &match_count);
        } else if (req->index_to_use->lookup_exact) {
            matched_rows = req->index_to_use->lookup_exact(req->index_to_use->index_state, eval_exacts, req->num_index_values, &match_count);
        }

        for (size_t i = 0; i < match_count; i++) {
            current_row_set[t_idx] = matched_rows[i];

            if (local_filter) {
                sql_node_t *res = sql_eval(ctx, local_filter);
                if (!res || res->is_null || !res->value.bool_value) continue;
            }
            if (join_cond) {
                sql_node_t *res = sql_eval(ctx, join_cond);
                if (!res || res->is_null || !res->value.bool_value) continue;
            }

            matched_any = true;
            execute_nested_loop(ctx, compiled, num_datasets, step + 1, exec_order, current_row_set,
                                datasets, req_array, local_filters, step_join_conds, step_join_types, global_filter,
                                group_map_root, global_group, rs, join_maps, join_probe_exprs);
        }

        if (!matched_any && step_join_types[step] == JOIN_LEFT) {
            current_row_set[t_idx] = NULL;
            execute_nested_loop(ctx, compiled, num_datasets, step + 1, exec_order, current_row_set,
                                datasets, req_array, local_filters, step_join_conds, step_join_types, global_filter,
                                group_map_root, global_group, rs, join_maps, join_probe_exprs);
        }
        return;
    }

    size_t mat_idx = 0;
    void *raw_row = NULL;

    if (ds->mode == DS_MODE_STREAMING && ds->rewind) {
        if (step > 0) VM_DEBUG("[VM-WARN] FATAL HIT: Rewinding streaming table '%s' in inner loop!\n", ds->alias);
        ds->rewind(ds);
    }

    while (true) {
        if (ds->mode == DS_MODE_MATERIALIZED) {
            if (mat_idx >= ds->count) break;
            raw_row = ds->rows[mat_idx++];
        } else {
            if (!ds->next(ds, &raw_row)) break;
        }

        current_row_set[t_idx] = raw_row;

        if (local_filter) {
            sql_node_t *res = sql_eval(ctx, local_filter);
            if (!res || res->is_null || !res->value.bool_value) continue;
        }
        if (join_cond) {
            sql_node_t *res = sql_eval(ctx, join_cond);
            if (!res || res->is_null || !res->value.bool_value) continue;
        }

        matched_any = true;
        execute_nested_loop(ctx, compiled, num_datasets, step + 1, exec_order, current_row_set,
                            datasets, req_array, local_filters, step_join_conds, step_join_types, global_filter,
                            group_map_root, global_group, rs, join_maps, join_probe_exprs);
    }

    if (!matched_any && step_join_types[step] == JOIN_LEFT) {
        current_row_set[t_idx] = NULL;
        execute_nested_loop(ctx, compiled, num_datasets, step + 1, exec_order, current_row_set,
                            datasets, req_array, local_filters, step_join_conds, step_join_types, global_filter,
                            group_map_root, global_group, rs, join_maps, join_probe_exprs);
    }
}

typedef struct {
    sql_window_plan_t *wp;
    size_t cache_offset_part;
    size_t cache_offset_sort;
} win_sort_cfg_t;

static int window_row_comparator(const void *a, const void *b, void *arg) {
    win_sort_cfg_t *cfg = (win_sort_cfg_t *)arg;
    sql_result_row_t *ra = *(sql_result_row_t **)a;
    sql_result_row_t *rb = *(sql_result_row_t **)b;

    for(size_t i=0; i<cfg->wp->num_partition_keys; i++) {
        int cmp = compare_sql_nodes(ra->window_cache[cfg->cache_offset_part + i], rb->window_cache[cfg->cache_offset_part + i]);
        if (cmp != 0) return cmp;
    }
    for(size_t i=0; i<cfg->wp->num_sort_keys; i++) {
        int cmp = compare_sql_nodes(ra->window_cache[cfg->cache_offset_sort + i], rb->window_cache[cfg->cache_offset_sort + i]);
        if (cmp != 0) return cmp * cfg->wp->sort_directions[i];
    }
    return 0;
}

static inline
_macro_sort(internal_window_sort, cmp_arg, sql_result_row_t*, window_row_comparator)

static void sql_execute_windows(sql_ctx_t *ctx, sql_result_set_t *rs, sql_compiled_query_t *compiled) {
    if (compiled->num_window_plans == 0 || rs->count == 0) return;

    sql_result_row_t **row_ptrs = aml_pool_alloc(ctx->pool, rs->count * sizeof(sql_result_row_t *));
    size_t global_cache_offset = 0;

    for (size_t w = 0; w < compiled->num_window_plans; w++) {
        sql_window_plan_t *wp = compiled->window_plans[w];
        for(size_t i=0; i<rs->count; i++) row_ptrs[i] = &rs->rows[i];

        win_sort_cfg_t cfg;
        cfg.wp = wp;
        cfg.cache_offset_part = global_cache_offset;
        cfg.cache_offset_sort = global_cache_offset + wp->num_partition_keys;
        size_t cache_offset_args = cfg.cache_offset_sort + wp->num_sort_keys;

        internal_window_sort(row_ptrs, rs->count, &cfg);

        void *state = aml_pool_zalloc(ctx->pool, wp->func_node->spec->state_size);
        wp->func_node->spec->agg_init(state);

        sql_node_t **orig_params = aml_pool_alloc(ctx->pool, wp->func_node->num_parameters * sizeof(sql_node_t *));
        for(size_t p=0; p<wp->func_node->num_parameters; p++) orig_params[p] = wp->func_node->parameters[p];

        for(size_t i=0; i<rs->count; i++) {
            sql_result_row_t *row = row_ptrs[i];
            sql_result_row_t *prev = i > 0 ? row_ptrs[i-1] : NULL;

            bool partition_changed = false;
            if (prev) {
                for(size_t k=0; k<wp->num_partition_keys; k++) {
                    if (compare_sql_nodes(row->window_cache[cfg.cache_offset_part + k], prev->window_cache[cfg.cache_offset_part + k]) != 0) {
                        partition_changed = true; break;
                    }
                }
            } else {
                partition_changed = true;
            }

            if (partition_changed && i > 0) {
                wp->func_node->spec->agg_init(state);
            }

            bool tie = false;
            if (!partition_changed && i > 0 && wp->num_sort_keys > 0) {
                tie = true;
                for(size_t k=0; k<wp->num_sort_keys; k++) {
                    if (compare_sql_nodes(row->window_cache[cfg.cache_offset_sort + k], prev->window_cache[cfg.cache_offset_sort + k]) != 0) {
                        tie = false; break;
                    }
                }
            }

            if (strcasecmp(wp->func_node->spec->name, "RANK") == 0) {
                typedef struct { int current_rank; int rows_in_partition; } rank_state_t;
                rank_state_t *rs_state = (rank_state_t *)state;
                if (!tie && !partition_changed && i > 0) {
                    rs_state->current_rank = rs_state->rows_in_partition + 1;
                }
            }

            for(size_t p=0; p<wp->func_node->num_parameters; p++) {
                wp->func_node->parameters[p] = row->window_cache[cache_offset_args + p];
            }

            wp->func_node->spec->agg_step(ctx, wp->func_node, state);
            row->columns[wp->projection_index] = wp->func_node->spec->agg_finalize(ctx, wp->func_node, state);
        }

        for(size_t p=0; p<wp->func_node->num_parameters; p++) wp->func_node->parameters[p] = orig_params[p];

        global_cache_offset += wp->num_partition_keys + wp->num_sort_keys + wp->func_node->num_parameters;
    }
}

static sql_dataset_t *internal_execute(sql_vm_t *vm, sql_select_t *ast, const char *forced_alias, sql_dataset_t **parent_ctes, size_t num_parent_ctes, bool explain_mode) {
    if (!ast) return NULL;
    sql_ctx_t *ctx = vm->ctx;

    /*if (!explain_mode) {
        VM_DEBUG("\n========================================\n");
        VM_DEBUG("[VM-INIT] Executing Sub-Tree: %s\n", forced_alias);
    }*/

    size_t num_local_ctes = 0;
    sql_cte_t *cte = ast->ctes;
    while(cte) { num_local_ctes++; cte = cte->next; }

    size_t num_available_ctes = num_parent_ctes + num_local_ctes;
    sql_dataset_t **available_ctes = NULL;

    if (num_available_ctes > 0) {
        available_ctes = aml_pool_alloc(ctx->pool, num_available_ctes * sizeof(sql_dataset_t *));
        for(size_t i = 0; i < num_parent_ctes; i++) available_ctes[i] = parent_ctes[i];

        size_t cte_idx = num_parent_ctes;
        cte = ast->ctes;
        while(cte) {
            if (!explain_mode) VM_DEBUG("[VM-INIT] Evaluating CTE '%s'\n", cte->alias);
            sql_dataset_t *cte_ds = internal_execute(vm, cte->query, cte->alias, available_ctes, cte_idx, explain_mode);
            if (!cte_ds) return NULL;
            cte_ds->table_name = aml_pool_strdup(ctx->pool, cte->alias);
            available_ctes[cte_idx++] = cte_ds;
            cte = cte->next;
        }
    }

    sql_dataset_t **datasets = aml_pool_alloc(ctx->pool, 16 * sizeof(sql_dataset_t *));
    size_t num_datasets = 0;

    if (ast->subquery) {
        datasets[num_datasets] = internal_execute(vm, ast->subquery, ast->table_alias ? ast->table_alias : "subquery", available_ctes, num_available_ctes, explain_mode);
        datasets[num_datasets]->table_name = "subquery";
        num_datasets++;
    } else if (ast->table) {
        sql_dataset_t *matched_cte = NULL;
        for(size_t c=0; c<num_available_ctes; c++) {
            if(strcasecmp(ast->table, available_ctes[c]->alias) == 0) {
                matched_cte = available_ctes[c];
                break;
            }
        }

        if (matched_cte) {
            if (!explain_mode) VM_DEBUG("[SCHEMA] Binding CTE '%s'\n", ast->table);
            datasets[num_datasets] = aml_pool_alloc(ctx->pool, sizeof(sql_dataset_t));
            *datasets[num_datasets] = *matched_cte;
            datasets[num_datasets]->alias = ast->table_alias ? ast->table_alias : matched_cte->alias;
            num_datasets++;
        } else {
            datasets[num_datasets] = vm->fetch_table(vm, ast->table);
            if (datasets[num_datasets]) {
                datasets[num_datasets]->table_name = ast->table;
                datasets[num_datasets]->alias = ast->table_alias ? ast->table_alias : ast->table;
                num_datasets++;
            }
        }
    }

    sql_join_t *j = ast->joins;
    while (j) {
        if (j->subquery) {
            datasets[num_datasets] = internal_execute(vm, j->subquery, j->alias ? j->alias : "subquery", available_ctes, num_available_ctes, explain_mode);
            datasets[num_datasets]->table_name = "subquery";
            num_datasets++;
        } else if (j->table) {
            sql_dataset_t *matched_cte = NULL;
            for(size_t c=0; c<num_available_ctes; c++) {
                if(strcasecmp(j->table, available_ctes[c]->alias) == 0) {
                    matched_cte = available_ctes[c];
                    break;
                }
            }

            if (matched_cte) {
                if (!explain_mode) VM_DEBUG("[SCHEMA] Binding CTE '%s'\n", j->table);
                datasets[num_datasets] = aml_pool_alloc(ctx->pool, sizeof(sql_dataset_t));
                *datasets[num_datasets] = *matched_cte;
                datasets[num_datasets]->alias = j->alias ? j->alias : matched_cte->alias;
                num_datasets++;
            } else {
                datasets[num_datasets] = vm->fetch_table(vm, j->table);
                if (datasets[num_datasets]) {
                    datasets[num_datasets]->table_name = j->table;
                    datasets[num_datasets]->alias = j->alias ? j->alias : j->table;
                    num_datasets++;
                }
            }
        }
        j = j->next;
    }

    vm_catalog_state_t *cs = aml_pool_alloc(ctx->pool, sizeof(vm_catalog_state_t));
    cs->vm = vm;
    cs->datasets = datasets;
    cs->num_datasets = num_datasets;

    void *old_catalog_state = ctx->catalog_state;
    void *old_row = ctx->row;
    void **old_agg_states = ctx->current_agg_states;
    ctx->catalog_state = cs;

    if (ast->is_star) {
        sql_ast_node_t *head = NULL, *tail = NULL;
        bool can_expand = true;
        for (size_t i = 0; i < cs->num_datasets; i++) {
            if (!cs->datasets[i]->is_virtual) {
                can_expand = false;
                break;
            }
        }

        if (can_expand) {
            for (size_t i = 0; i < cs->num_datasets; i++) {
                sql_dataset_t *ds = cs->datasets[i];
                for (size_t c = 0; c < ds->num_columns; c++) {
                    sql_ast_node_t *col_node = aml_pool_zalloc(ctx->pool, sizeof(sql_ast_node_t));
                    col_node->type = SQL_IDENTIFIER;
                    col_node->value = aml_pool_strdupf(ctx->pool, "%s.%s", ds->alias, ds->column_names[c]);
                    col_node->alias = aml_pool_strdup(ctx->pool, ds->column_names[c]);

                    if (!head) head = tail = col_node;
                    else { tail->next = col_node; tail = col_node; }
                }
            }
            ast->is_star = false;
            ast->columns = head;
        } else {
            sql_ctx_error(ctx, "Engine Limitation: SELECT * is currently only supported on subqueries and CTEs.");
            ctx->catalog_state = old_catalog_state;
            return NULL;
        }
    }

    bind_window_expressions(ctx, ast, cs);

    if (!sql_bind_query_extended(ctx, ast)) {
        ctx->catalog_state = old_catalog_state;
        ctx->row = old_row;
        ctx->current_agg_states = old_agg_states;
        return NULL;
    }

    sql_execution_plan_t *plan = sql_plan_query(ctx, ast);
    sql_pushdown_filters(ctx, plan);

    int *exec_order = aml_pool_alloc(ctx->pool, num_datasets * sizeof(int));
    for (int i = 0; i < num_datasets; i++) exec_order[i] = i;

    if (!explain_mode) VM_DEBUG("[VM-OPT] Running Universal Reordering Pass...\n");
    bool changed = true;
    while (changed) {
        changed = false;
        sql_join_plan_t *jp = plan->joins;
        while (jp) {
            int r_table = jp->right_table_index;

            if (jp->join_type == JOIN_INNER && jp->on_condition && jp->on_condition->left && jp->on_condition->right) {
                sql_node_t *l_cond = sql_compile_expression(ctx, jp->on_condition->left);
                sql_node_t *r_cond = sql_compile_expression(ctx, jp->on_condition->right);

                int left_dep = -2, right_dep = -2;
                check_node_dep(l_cond, &left_dep);
                check_node_dep(r_cond, &right_dep);

                int l_table = (left_dep == r_table) ? right_dep : left_dep;

                if (l_table >= 0) {
                    int idx_L = -1, idx_R = -1;
                    for (int i=0; i<num_datasets; i++) {
                        if (exec_order[i] == l_table) idx_L = i;
                        if (exec_order[i] == r_table) idx_R = i;
                    }
                    if (idx_L == idx_R - 1) {
                        long score_L = (datasets[l_table]->mode == DS_MODE_STREAMING ? 1000000000 : 0) + datasets[l_table]->count;
                        long score_R = (datasets[r_table]->mode == DS_MODE_STREAMING ? 1000000000 : 0) + datasets[r_table]->count;
                        if (score_L < score_R) {
                            if (!explain_mode) VM_DEBUG("  -> SWAP: Bubbling massive table '%s' to outer loop, tiny '%s' to inner loop.\n",
                                     datasets[r_table]->alias, datasets[l_table]->alias);
                            exec_order[idx_L] = r_table;
                            exec_order[idx_R] = l_table;
                            changed = true;
                        }
                    }
                }
            }
            jp = jp->next;
        }
    }

    sql_node_t **step_join_conds = aml_pool_zalloc(ctx->pool, num_datasets * sizeof(sql_node_t *));
    sql_join_type_t *step_join_types = aml_pool_zalloc(ctx->pool, num_datasets * sizeof(sql_join_type_t));

    for(size_t i = 0; i < num_datasets; i++) step_join_types[i] = JOIN_INNER;

    sql_join_plan_t *jp_pass = plan->joins;
    while(jp_pass) {
        sql_node_t *compiled_cond = sql_compile_expression(ctx, jp_pass->on_condition);
        int max_step = 0;
        for (int i=0; i<num_datasets; i++) {
            if (node_depends_on_table(compiled_cond, exec_order[i])) {
                if (i > max_step) max_step = i;
            }
        }
        if (!explain_mode) VM_DEBUG("[VM-MAP] Join condition assigned to Execution Step %d (Table '%s')\n",
                 max_step, datasets[exec_order[max_step]]->alias);
        step_join_conds[max_step] = compiled_cond;
        step_join_types[max_step] = jp_pass->join_type;
        jp_pass = jp_pass->next;
    }

    sql_node_t **local_filters = aml_pool_zalloc(ctx->pool, num_datasets * sizeof(sql_node_t *));
    sql_table_request_t **req_array = aml_pool_zalloc(ctx->pool, num_datasets * sizeof(sql_table_request_t *));

    sql_table_request_t *req = plan->table_requests;
    while(req) {
        req_array[req->table_index] = req;
        local_filters[req->table_index] = sql_compile_expression(ctx, req->table_filters);

        optimize_indexes_for_table(ctx, req, datasets[req->table_index], local_filters[req->table_index]);

        req = req->next;
    }

    sql_compiled_query_t *compiled = sql_compile_query(ctx, ast);

    // --- FULL EXPLAIN PIPELINE OUTPUT ---
    if (explain_mode) {
        aml_buffer_t *buf = aml_buffer_pool_init(ctx->pool, 2048);

        for (size_t c = num_parent_ctes; c < num_available_ctes; c++) {
            if (available_ctes[c]->rs && available_ctes[c]->rs->explain_output) {
                aml_buffer_appends(buf, available_ctes[c]->rs->explain_output);
                aml_buffer_appends(buf, "\n");
            }
        }

        aml_buffer_appendf(buf, "=== EXPLAIN PLAN FOR: %s ===\n", forced_alias);
        sql_print_plan(buf, ctx, plan);
        aml_buffer_appendf(buf, "\n--- JOIN EXECUTION ORDER ---\n");
        for (int i = 0; i < num_datasets; i++) {
            aml_buffer_appendf(buf, "%d. %s (%s)\n", i + 1, datasets[exec_order[i]]->table_name, datasets[exec_order[i]]->alias);
        }

        if (ast->group_by) {
            aml_buffer_appendf(buf, "\n--- 4. GROUPING ---\n");
            aml_buffer_appendf(buf, "GROUP BY: ");
            sql_ast_node_t *gb = ast->group_by;
            while(gb) {
                char *str = sql_ast_to_string(ctx, gb);
                aml_buffer_appendf(buf, "%s%s", str, gb->next ? ", " : "");
                gb = gb->next;
            }
            aml_buffer_appendf(buf, "\n");
        }

        if (ast->having_clause) {
            char *hav_str = sql_ast_to_string(ctx, ast->having_clause);
            aml_buffer_appendf(buf, "HAVING: %s\n", hav_str);
        }

        if (compiled->num_window_plans > 0) {
            aml_buffer_appendf(buf, "\n--- 5. WINDOW EXECUTION PHASE ---\n");
            for (size_t w = 0; w < compiled->num_window_plans; w++) {
                aml_buffer_appendf(buf, "Evaluate Window: [%zu]\n", w);
            }
        }

        if (ast->order_by) {
            aml_buffer_appendf(buf, "\n--- 6. SORTING ---\n");
            aml_buffer_appendf(buf, "ORDER BY: ");
            sql_order_by_t *ob = ast->order_by;
            while(ob) {
                char *str = sql_ast_to_string(ctx, ob->expr);
                aml_buffer_appendf(buf, "%s %s%s", str, ob->is_desc ? "DESC" : "ASC", ob->next ? ", " : "");
                ob = ob->next;
            }
            aml_buffer_appendf(buf, "\n");
        }

        if (ast->limit || ast->offset) {
            aml_buffer_appendf(buf, "\n--- 7. TRUNCATION ---\n");
            if (ast->limit) {
                char *lim_str = sql_ast_to_string(ctx, ast->limit);
                aml_buffer_appendf(buf, "LIMIT: %s\n", lim_str);
            }
            if (ast->offset) {
                char *off_str = sql_ast_to_string(ctx, ast->offset);
                aml_buffer_appendf(buf, "OFFSET: %s\n", off_str);
            }
        }

        aml_buffer_appendf(buf, "===============================\n");
        aml_buffer_appendc(buf, '\0');

        for (size_t i = 0; i < num_datasets; i++) {
            if (datasets[i]->mode == DS_MODE_STREAMING && datasets[i]->close) {
                datasets[i]->close(datasets[i]);
            }
        }

        ctx->catalog_state = old_catalog_state;
        ctx->row = old_row;
        ctx->current_agg_states = old_agg_states;

        sql_dataset_t *out_ds = aml_pool_zalloc(ctx->pool, sizeof(sql_dataset_t));
        out_ds->is_virtual = true;
        out_ds->mode = DS_MODE_MATERIALIZED;
        out_ds->rs = sql_result_set_init(ctx->pool, 0, NULL, 0, NULL, NULL);
        out_ds->alias = forced_alias;

        out_ds->num_columns = compiled->num_projections;
        out_ds->column_names = compiled->display_names;
        out_ds->column_types = aml_pool_alloc(ctx->pool, out_ds->num_columns * sizeof(sql_data_type_t));
        for (size_t c = 0; c < out_ds->num_columns; c++) {
            out_ds->column_types[c] = compiled->projections[c]->data_type;
        }

        out_ds->rs->explain_output = aml_buffer_data(buf);
        return out_ds;
    }

    macro_map_t **join_maps = aml_pool_zalloc(ctx->pool, num_datasets * sizeof(macro_map_t *));
    sql_node_t **join_probe_exprs = aml_pool_zalloc(ctx->pool, num_datasets * sizeof(sql_node_t *));

    sql_join_plan_t *hj_pass = plan->joins;
    while (hj_pass) {
        if (hj_pass->algorithm == JOIN_ALGO_HASH_JOIN && hj_pass->on_condition && hj_pass->on_condition->left && hj_pass->on_condition->right) {

            sql_node_t *left_compiled = sql_compile_expression(ctx, hj_pass->on_condition->left);
            sql_node_t *right_compiled = sql_compile_expression(ctx, hj_pass->on_condition->right);

            int left_dep = -2, right_dep = -2;
            check_node_dep(left_compiled, &left_dep);
            check_node_dep(right_compiled, &right_dep);

            if (left_dep >= 0 && right_dep >= 0 && left_dep != right_dep) {
                int step_L = -1, step_R = -1;
                for (int i=0; i<num_datasets; i++) {
                    if (exec_order[i] == left_dep) step_L = i;
                    if (exec_order[i] == right_dep) step_R = i;
                }

                int max_step = (step_L > step_R) ? step_L : step_R;
                int build_table = exec_order[max_step];

                sql_node_t *build_expr = (build_table == left_dep) ? left_compiled : right_compiled;
                sql_node_t *probe_expr = (build_table == left_dep) ? right_compiled : left_compiled;

                join_probe_exprs[max_step] = probe_expr;

                sql_dataset_t *ds = datasets[build_table];
                if (ds->mode == DS_MODE_STREAMING && ds->rewind) ds->rewind(ds);

                size_t mat_idx = 0;
                void *raw_row = NULL;

                void *saved_row = ctx->row;
                void *temp_row_set[16] = {0};
                ctx->row = temp_row_set;

                VM_DEBUG("[VM-BUILD] Generating Hash Map for Inner Table '%s' at Step %d...\n", ds->alias, max_step);
                size_t build_count = 0;

                sql_node_t *local_filter_compiled = local_filters[build_table];

                while (true) {
                    if (ds->mode == DS_MODE_MATERIALIZED) {
                        if (mat_idx >= ds->count) break;
                        raw_row = ds->rows[mat_idx++];
                    } else {
                        if (!ds->next(ds, &raw_row)) break;
                    }

                    build_count++;
                    temp_row_set[build_table] = raw_row;

                    if (local_filter_compiled) {
                        sql_node_t *res = sql_eval(ctx, local_filter_compiled);
                        if (!res || res->is_null || !res->value.bool_value) continue;
                    }

                    sql_node_t *key_val = sql_eval(ctx, build_expr);
                    if (!key_val || key_val->is_null) continue;

                    group_node_t lookup = {0};
                    lookup.key = key_val;
                    group_node_t *found = group_map_find(join_maps[max_step], &lookup);
                    if (!found) {
                        found = aml_pool_zalloc(ctx->pool, sizeof(group_node_t));
                        found->key = copy_evaluated_node(ctx, key_val);
                        group_map_insert(&join_maps[max_step], found);
                    }

                    join_row_t *jr = aml_pool_zalloc(ctx->pool, sizeof(join_row_t));

                    // --- CLONE ROW FIX FOR HASH MAP BUILDING ---
                    if (ds->clone_row) {
                        jr->row = ds->clone_row(ds, raw_row, ctx->pool);
                    } else {
                        jr->row = raw_row;
                    }

                    jr->next = found->rows;
                    found->rows = jr;
                }

                ctx->row = saved_row;

                VM_DEBUG("[VM-BUILD] Completed. Indexed %zu rows.\n", build_count);
                step_join_conds[max_step] = NULL;
            }
        }
        hj_pass = hj_pass->next;
    }

    sql_node_t *global_filter = sql_compile_expression(ctx, plan->global_filters);

    sql_result_set_t *rs = sql_result_set_init(ctx->pool, compiled->num_projections, compiled->display_names, compiled->num_sort_keys, compiled->sort_directions, compiled->sort_projection_indices);

    size_t win_cache_size = 0;
    for(size_t w=0; w < compiled->num_window_plans; w++) {
        win_cache_size += compiled->window_plans[w]->num_partition_keys;
        win_cache_size += compiled->window_plans[w]->num_sort_keys;
        win_cache_size += compiled->window_plans[w]->func_node->num_parameters;
    }
    rs->window_cache_size = win_cache_size;

    macro_map_t *group_map_root = NULL;
    group_node_t *global_group = NULL;
    void **current_row_set = aml_pool_zalloc(ctx->pool, num_datasets * sizeof(void *));
    ctx->row = current_row_set;

    VM_DEBUG("[VM-EXEC] Starting Final Pipeline...\n");
    execute_nested_loop(ctx, compiled, num_datasets, 0, exec_order, current_row_set,
                        datasets, req_array, local_filters, step_join_conds, step_join_types, global_filter,
                        &group_map_root, &global_group, rs, join_maps, join_probe_exprs);

    if (compiled->num_group_keys > 0) {
        for (macro_map_t *p = macro_map_first(group_map_root); p; p = macro_map_next(p)) {
            group_node_t *group = (group_node_t *)p;
            ctx->current_agg_states = group->agg_states;
            ctx->row = group->first_row_set;
            if (compiled->having_filter) {
                sql_node_t *res = sql_eval(ctx, compiled->having_filter);
                if (!res || res->is_null || !res->value.bool_value) continue;
            }
            sql_result_set_append(ctx, rs, compiled);
        }
    } else if (global_group) {
        ctx->current_agg_states = global_group->agg_states;
        ctx->row = global_group->first_row_set;
        bool pass = true;
        if (compiled->having_filter) {
            sql_node_t *res = sql_eval(ctx, compiled->having_filter);
            if (!res || res->is_null || !res->value.bool_value) pass = false;
        }
        if (pass) sql_result_set_append(ctx, rs, compiled);
    }

    sql_execute_windows(ctx, rs, compiled);

    sql_result_set_sort(rs);

    size_t offset_val = 0;
    if (compiled->offset) {
        sql_node_t *off_res = sql_eval(ctx, compiled->offset);
        if (off_res && !off_res->is_null && off_res->data_type == SQL_TYPE_INT && off_res->value.int_value > 0) {
            offset_val = (size_t)off_res->value.int_value;
        }
    }

    size_t limit_val = rs->count;
    if (compiled->limit) {
        sql_node_t *lim_res = sql_eval(ctx, compiled->limit);
        if (lim_res && !lim_res->is_null && lim_res->data_type == SQL_TYPE_INT && lim_res->value.int_value >= 0) {
            limit_val = (size_t)lim_res->value.int_value;
        }
    }

    if (offset_val > 0) {
        if (offset_val >= rs->count) {
            rs->count = 0;
        } else {
            rs->count -= offset_val;
            memmove(rs->rows, rs->rows + offset_val, rs->count * sizeof(sql_result_row_t));
        }
    }

    if (limit_val < rs->count) {
        rs->count = limit_val;
    }

    for (size_t i = 0; i < num_datasets; i++) {
        if (datasets[i]->mode == DS_MODE_STREAMING && datasets[i]->close) {
            datasets[i]->close(datasets[i]);
        }
    }

    ctx->catalog_state = old_catalog_state;
    ctx->row = old_row;
    ctx->current_agg_states = old_agg_states;

    sql_dataset_t *out_ds = aml_pool_zalloc(ctx->pool, sizeof(sql_dataset_t));
    out_ds->is_virtual = true;
    out_ds->mode = DS_MODE_MATERIALIZED;
    out_ds->rs = rs;
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

// Rename the existing function to _sql_vm_execute
static sql_result_set_t *_sql_vm_execute(sql_vm_t *vm, sql_select_t *ast) {
    if (!ast) return NULL;

    sql_dataset_t *final_ds = internal_execute(vm, ast, "final_results", NULL, 0, ast->is_explain);

    if (!final_ds || !final_ds->is_virtual) return NULL;
    return final_ds->rs;
}

// Create the shiny new public API
sql_result_set_t *sql_vm_execute(sql_vm_t *vm, sql_select_t *ast) {
    // 1. Run the core engine
    sql_result_set_t *rs = _sql_vm_execute(vm, ast);

    // 2. Safely destroy all auxiliary pools generated during THIS specific execution layer
    sql_ctx_destroy_tracked_pools(vm->ctx);

    // 3. Return the result
    return rs;
}
