// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0

#include "sql-parser-library/sql_result_set.h"
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
sql_result_set_t *sql_result_set_init(aml_pool_t *pool, size_t num_columns, const char **column_names, size_t num_sort_keys, int *sort_directions) {
    sql_result_set_t *rs = aml_pool_zalloc(pool, sizeof(sql_result_set_t));
    rs->pool = pool;
    rs->num_columns = num_columns;
    rs->column_names = column_names; // <-- NEW
    rs->num_sort_keys = num_sort_keys;
    rs->sort_directions = sort_directions;

    rs->capacity = 16;
    rs->count = 0;
    rs->rows = aml_pool_alloc(pool, rs->capacity * sizeof(sql_result_row_t));

    return rs;
}

// --- APPEND LOGIC ---
void sql_result_set_append(sql_ctx_t *ctx, sql_result_set_t *rs, sql_node_t **projections, sql_node_t **sort_exprs) {
    if (rs->count >= rs->capacity) {
        size_t new_capacity = rs->capacity * 2;
        sql_result_row_t *new_rows = aml_pool_alloc(rs->pool, new_capacity * sizeof(sql_result_row_t));
        memcpy(new_rows, rs->rows, rs->count * sizeof(sql_result_row_t));
        rs->rows = new_rows;
        rs->capacity = new_capacity;
    }

    sql_result_row_t *new_row = &rs->rows[rs->count++];

    if (rs->num_columns > 0) {
        new_row->columns = aml_pool_alloc(rs->pool, rs->num_columns * sizeof(sql_node_t *));
        for (size_t p = 0; p < rs->num_columns; p++) {
            sql_node_t *val = sql_eval(ctx, projections[p]);
            new_row->columns[p] = copy_evaluated_node(ctx, val);
        }
    }

    if (rs->num_sort_keys > 0) {
        new_row->sort_keys = aml_pool_alloc(rs->pool, rs->num_sort_keys * sizeof(sql_node_t *));
        for (size_t s = 0; s < rs->num_sort_keys; s++) {
            sql_node_t *val = sql_eval(ctx, sort_exprs[s]);
            new_row->sort_keys[s] = copy_evaluated_node(ctx, val);
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
        case SQL_TYPE_INT:
            return (a->value.int_value > b->value.int_value) - (a->value.int_value < b->value.int_value);
        case SQL_TYPE_DOUBLE:
            return (a->value.double_value > b->value.double_value) - (a->value.double_value < b->value.double_value);
        case SQL_TYPE_STRING:
            return strcmp(a->value.string_value, b->value.string_value);
        case SQL_TYPE_DATETIME:
            return (a->value.epoch > b->value.epoch) - (a->value.epoch < b->value.epoch);
        case SQL_TYPE_BOOL:
            return (a->value.bool_value > b->value.bool_value) - (a->value.bool_value < b->value.bool_value);
        default: return 0;
    }
}

typedef struct {
    int *directions;
    size_t num_keys;
} sort_config_t;

static int result_row_comparator(const sql_result_row_t *row_a, const sql_result_row_t *row_b, void *arg) {
    sort_config_t *cfg = (sort_config_t *)arg;
    for (size_t i = 0; i < cfg->num_keys; i++) {
        int cmp = compare_sql_nodes(row_a->sort_keys[i], row_b->sort_keys[i]);
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
        .num_keys = rs->num_sort_keys
    };
    internal_rs_sort(rs->rows, rs->count, &cfg);
}
