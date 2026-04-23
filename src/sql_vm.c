// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0

#include "sql-parser-library/sql_vm.h"
#include "sql-parser-library/sql_compiler.h"
#include "sql-parser-library/sql_binder.h"
#include "the-macro-library/macro_map.h"
#include <string.h>
#include <stdio.h>

// --- VM DEBUGGING MACROS ---
#define ENABLE_VM_DEBUG
#ifdef ENABLE_VM_DEBUG
    #define VM_DEBUG(...) printf(__VA_ARGS__)
#else
    #define VM_DEBUG(...) do {} while (0)
#endif

// --- VM CONTEXT ---
typedef struct {
    sql_vm_t *vm;
    sql_dataset_t **datasets;
    size_t num_datasets;
} vm_catalog_state_t;

// Forward declaration for recursion
static sql_dataset_t *internal_execute(sql_vm_t *vm, sql_select_t *ast, const char *forced_alias, sql_dataset_t **parent_ctes, size_t num_parent_ctes);

// --- VIRTUAL TABLE ROUTER ---
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
    void **row_set = (void **)ctx->row;
    if (!row_set || !f->column) return sql_bool_init(ctx, false, true);

    int t_idx = f->column->table_index;
    void *raw_row = row_set[t_idx];
    if (!raw_row) return sql_bool_init(ctx, false, true);

    vm_catalog_state_t *cs = (vm_catalog_state_t *)ctx->catalog_state;
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
                    return col;
                }
            }
        } else {
            const char *physical_name = ds->table_name ? ds->table_name : ds->alias;
            sql_ctx_column_t *col = vm->resolve_column(vm, physical_name, column_name);
            if (col) {
                col->table_name = aml_pool_strdup(ctx->pool, ds->alias);
                return col;
            }
        }
    }
    return NULL;
}

// --- VM INITIALIZATION ---
sql_vm_t *sql_vm_init(sql_ctx_t *ctx, sql_vm_fetch_table_cb fetch_cb, sql_vm_resolve_column_cb resolve_cb, void *user_data) {
    sql_vm_t *vm = aml_pool_zalloc(ctx->pool, sizeof(sql_vm_t));
    vm->ctx = ctx;
    vm->pool = ctx->pool;
    vm->fetch_table = fetch_cb;
    vm->resolve_column = resolve_cb;
    vm->user_data = user_data;
    ctx->schema_lookup = vm_catalog_router;
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

// --- MACRO MAP GROUPING ---
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
macro_map_insert(group_map_insert, group_node_t, group_node_cmp);
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

// --- DUAL-MODE NESTED LOOP EXECUTOR ---
static void execute_nested_loop(sql_ctx_t *ctx, sql_compiled_query_t *compiled,
                         int num_datasets, int step, int *exec_order, void **current_row_set,
                         sql_dataset_t **datasets, sql_node_t **local_filters, sql_node_t **step_join_conds,
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
        } else if (compiled->num_aggregates > 0) {
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

    int t_idx = exec_order[step];

    // --- HASH JOIN PROBE PHASE ---
    if (join_maps && join_maps[step]) {
        sql_node_t *probe_key = sql_eval(ctx, join_probe_exprs[step]);

        if (probe_key && !probe_key->is_null) {
            group_node_t lookup = {0};
            lookup.key = probe_key;
            group_node_t *found = group_map_find(join_maps[step], &lookup);

            if (found) {
                join_row_t *jr = found->rows;
                while (jr) {
                    current_row_set[t_idx] = jr->row;
                    execute_nested_loop(ctx, compiled, num_datasets, step + 1, exec_order, current_row_set,
                                        datasets, local_filters, step_join_conds, global_filter,
                                        group_map_root, global_group, rs, join_maps, join_probe_exprs);
                    jr = jr->next;
                }
            }
        }
        return;
    }

    // --- STANDARD READ LOOP (NESTED LOOP FALLBACK) ---
    sql_dataset_t *ds = datasets[t_idx];
    sql_node_t *local_filter = local_filters[t_idx];
    sql_node_t *join_cond = step_join_conds[step];

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

        execute_nested_loop(ctx, compiled, num_datasets, step + 1, exec_order, current_row_set,
                            datasets, local_filters, step_join_conds, global_filter,
                            group_map_root, global_group, rs, join_maps, join_probe_exprs);
    }
}

// --- VM EXECUTION ---
static sql_dataset_t *internal_execute(sql_vm_t *vm, sql_select_t *ast, const char *forced_alias, sql_dataset_t **parent_ctes, size_t num_parent_ctes) {
    if (!ast) return NULL;
    sql_ctx_t *ctx = vm->ctx;

    VM_DEBUG("\n========================================\n");
    VM_DEBUG("[VM-INIT] Executing Sub-Tree: %s\n", forced_alias);

    // --- 1. EAGER-EVALUATE COMMON TABLE EXPRESSIONS (WITH) ---
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
            VM_DEBUG("[VM-INIT] Evaluating CTE '%s'\n", cte->alias);
            sql_dataset_t *cte_ds = internal_execute(vm, cte->query, cte->alias, available_ctes, cte_idx);
            if (!cte_ds) return NULL;
            cte_ds->table_name = aml_pool_strdup(ctx->pool, cte->alias); // Alias is its physical schema name
            available_ctes[cte_idx++] = cte_ds;
            cte = cte->next;
        }
    }

    sql_dataset_t **datasets = aml_pool_alloc(ctx->pool, 16 * sizeof(sql_dataset_t *));
    size_t num_datasets = 0;

    // --- BASE TABLE FETCHING ---
    if (ast->subquery) {
        datasets[num_datasets] = internal_execute(vm, ast->subquery, ast->table_alias ? ast->table_alias : "subquery", available_ctes, num_available_ctes);
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
            VM_DEBUG("[SCHEMA] Binding CTE '%s'\n", ast->table);
            datasets[num_datasets] = aml_pool_alloc(ctx->pool, sizeof(sql_dataset_t));
            *datasets[num_datasets] = *matched_cte; // Shallow copy allows independent aliases for the same memory blocks!
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

    // --- JOIN FETCHING ---
    sql_join_t *j = ast->joins;
    while (j) {
        if (j->subquery) {
            datasets[num_datasets] = internal_execute(vm, j->subquery, j->alias ? j->alias : "subquery", available_ctes, num_available_ctes);
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
                VM_DEBUG("[SCHEMA] Binding CTE '%s'\n", j->table);
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

    // --- 1. UNIVERSAL JOIN REORDERING OPTIMIZER ---
    VM_DEBUG("[VM-OPT] Running Universal Reordering Pass...\n");
    bool changed = true;
    while (changed) {
        changed = false;
        sql_join_plan_t *jp = plan->joins;
        while (jp) {
            int r_table = jp->right_table_index;
            if (jp->on_condition && jp->on_condition->left && jp->on_condition->right) {
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
                            VM_DEBUG("  -> SWAP: Bubbling massive table '%s' to outer loop, tiny '%s' to inner loop.\n",
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

    // --- 2. DYNAMIC CONDITION SCHEDULER ---
    sql_node_t **step_join_conds = aml_pool_zalloc(ctx->pool, num_datasets * sizeof(sql_node_t *));
    sql_join_plan_t *jp_pass = plan->joins;
    while(jp_pass) {
        sql_node_t *compiled_cond = sql_compile_expression(ctx, jp_pass->on_condition);
        int max_step = 0;
        for (int i=0; i<num_datasets; i++) {
            if (node_depends_on_table(compiled_cond, exec_order[i])) {
                if (i > max_step) max_step = i;
            }
        }
        VM_DEBUG("[VM-MAP] Join condition assigned to Execution Step %d (Table '%s')\n",
                 max_step, datasets[exec_order[max_step]]->alias);
        step_join_conds[max_step] = compiled_cond;
        jp_pass = jp_pass->next;
    }

    sql_node_t **local_filters = aml_pool_zalloc(ctx->pool, num_datasets * sizeof(sql_node_t *));
    sql_table_request_t *req = plan->table_requests;
    while(req) {
        local_filters[req->table_index] = sql_compile_expression(ctx, req->table_filters);
        req = req->next;
    }

    // --- 3. HASH JOIN BUILD PHASE ---
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
                    jr->row = raw_row;
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

    sql_compiled_query_t *compiled = sql_compile_query(ctx, ast);
    sql_node_t *global_filter = sql_compile_expression(ctx, plan->global_filters);

    // --- LAUNCH CORE EXECUTION ---
    sql_result_set_t *rs = sql_result_set_init(ctx->pool, compiled->num_projections, compiled->display_names, compiled->num_sort_keys, compiled->sort_directions);

    macro_map_t *group_map_root = NULL;
    group_node_t *global_group = NULL;
    void **current_row_set = aml_pool_zalloc(ctx->pool, num_datasets * sizeof(void *));
    ctx->row = current_row_set;

    VM_DEBUG("[VM-EXEC] Starting Final Pipeline...\n");
    execute_nested_loop(ctx, compiled, num_datasets, 0, exec_order, current_row_set,
                        datasets, local_filters, step_join_conds, global_filter,
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

sql_result_set_t *sql_vm_execute(sql_vm_t *vm, sql_select_t *ast) {
    sql_dataset_t *final_ds = internal_execute(vm, ast, "final_results", NULL, 0);
    if (!final_ds || !final_ds->is_virtual) return NULL;
    return final_ds->rs;
}
