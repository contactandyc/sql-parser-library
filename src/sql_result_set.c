// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_result_set.h"
#include "sql-parser-library/sql_compiler.h"
#include "the-macro-library/macro_sort.h"
#include <string.h>

static sql_node_t *copy_evaluated_node(sql_ctx_t *ctx, sql_node_t *src) {
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

// --- INITIALIZATION ---
sql_result_set_t *sql_result_set_init(aml_pool_t *pool, size_t num_columns, const char **column_names, size_t num_sort_keys, int *sort_directions, int *sort_projection_indices) {
    sql_result_set_t *rs = aml_pool_zalloc(pool, sizeof(sql_result_set_t));
    rs->pool = pool;
    rs->num_columns = num_columns;
    rs->column_names = column_names;
    rs->num_sort_keys = num_sort_keys;
    rs->sort_directions = sort_directions;
    rs->sort_projection_indices = sort_projection_indices;
    rs->explain_output = NULL;

    rs->capacity = 16;
    rs->count = 0;
    rs->rows = aml_pool_alloc(pool, rs->capacity * sizeof(sql_result_row_t));

    return rs;
}

// --- APPEND LOGIC ---
void sql_result_set_append(sql_ctx_t *ctx, sql_result_set_t *rs, struct sql_compiled_query_s *compiled) {
    if (rs->count >= rs->capacity) {
        size_t new_capacity = rs->capacity * 2;
        sql_result_row_t *new_rows = aml_pool_alloc(rs->pool, new_capacity * sizeof(sql_result_row_t));
        memcpy(new_rows, rs->rows, rs->count * sizeof(sql_result_row_t));
        rs->rows = new_rows;
        rs->capacity = new_capacity;
    }

    sql_result_row_t *new_row = &rs->rows[rs->count++];

    if (compiled->num_projections > 0) {
        new_row->columns = aml_pool_alloc(rs->pool, compiled->num_projections * sizeof(sql_node_t *));
        for (size_t p = 0; p < compiled->num_projections; p++) {
            bool is_window = false;
            for (size_t w = 0; w < compiled->num_window_plans; w++) {
                if (compiled->window_plans[w]->projection_index == p) {
                    is_window = true;
                    break;
                }
            }

            if (is_window) {
                new_row->columns[p] = sql_bool_init(ctx, false, true);
            } else {
                new_row->columns[p] = copy_evaluated_node(ctx, sql_eval(ctx, compiled->projections[p]));
            }
        }
    }

    if (compiled->num_sort_keys > 0) {
        new_row->sort_keys = aml_pool_alloc(rs->pool, compiled->num_sort_keys * sizeof(sql_node_t *));
        for (size_t s = 0; s < compiled->num_sort_keys; s++) {
            new_row->sort_keys[s] = copy_evaluated_node(ctx, sql_eval(ctx, compiled->sort_exprs[s]));
        }
    }

    if (compiled->num_window_plans > 0) {
        new_row->window_cache = aml_pool_alloc(rs->pool, rs->window_cache_size * sizeof(sql_node_t *));
        size_t idx = 0;

        for(size_t w=0; w < compiled->num_window_plans; w++) {
            sql_window_plan_t *wp = compiled->window_plans[w];
            for(size_t i=0; i<wp->num_partition_keys; i++) {
                new_row->window_cache[idx++] = copy_evaluated_node(ctx, sql_eval(ctx, wp->partition_exprs[i]));
            }
            for(size_t i=0; i<wp->num_sort_keys; i++) {
                new_row->window_cache[idx++] = copy_evaluated_node(ctx, sql_eval(ctx, wp->sort_exprs[i]));
            }
            for(size_t i=0; i<wp->func_node->num_parameters; i++) {
                new_row->window_cache[idx++] = copy_evaluated_node(ctx, sql_eval(ctx, wp->func_node->parameters[i]));
            }
        }
    }
}

// --- SORTING LOGIC ---
static int compare_sql_nodes(sql_node_t *a, sql_node_t *b) {
    if (a->is_null && b->is_null) return 0;
    if (a->is_null) return -1;
    if (b->is_null) return 1;

    if (a->data_type != b->data_type) return a->data_type - b->data_type;

    switch (a->data_type) {
        case SQL_TYPE_INT: return (a->value.int_value > b->value.int_value) - (a->value.int_value < b->value.int_value);
        case SQL_TYPE_DOUBLE: return (a->value.double_value > b->value.double_value) - (a->value.double_value < b->value.double_value);
        case SQL_TYPE_STRING: return strcmp(a->value.string_value, b->value.string_value);
        case SQL_TYPE_DATETIME: return (a->value.epoch > b->value.epoch) - (a->value.epoch < b->value.epoch);
        case SQL_TYPE_BOOL: return (a->value.bool_value > b->value.bool_value) - (a->value.bool_value < b->value.bool_value);
        default: return 0;
    }
}

typedef struct {
    int *directions;
    int *sort_projection_indices;
    size_t num_keys;
} sort_config_t;

static int result_row_comparator(const sql_result_row_t *row_a, const sql_result_row_t *row_b, void *arg) {
    sort_config_t *cfg = (sort_config_t *)arg;
    for (size_t i = 0; i < cfg->num_keys; i++) {
        sql_node_t *val_a = row_a->sort_keys[i];
        sql_node_t *val_b = row_b->sort_keys[i];

        // --- FIX: Intercept sorting to use the final materialized Projection! ---
        if (cfg->sort_projection_indices && cfg->sort_projection_indices[i] >= 0) {
            val_a = row_a->columns[cfg->sort_projection_indices[i]];
            val_b = row_b->columns[cfg->sort_projection_indices[i]];
        }

        int cmp = compare_sql_nodes(val_a, val_b);
        if (cmp != 0) return cmp * cfg->directions[i];
    }
    return 0;
}

static inline
_macro_sort(internal_rs_sort, cmp_arg, sql_result_row_t, result_row_comparator)

void sql_result_set_sort(sql_result_set_t *rs) {
    if (rs->num_sort_keys == 0 || rs->count < 2) return;
    sort_config_t cfg = {
        .directions = rs->sort_directions,
        .sort_projection_indices = rs->sort_projection_indices,
        .num_keys = rs->num_sort_keys
    };
    internal_rs_sort(rs->rows, rs->count, &cfg);
}
