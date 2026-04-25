// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_node.h"
#include "sql-parser-library/sql_ctx.h"

static void row_num_init(void *state) {
    *(int *)state = 0;
}

static void row_num_step(sql_ctx_t *ctx, sql_node_t *f, void *state) {
    *(int *)state += 1;
}

static sql_node_t *row_num_finalize(sql_ctx_t *ctx, sql_node_t *f, void *state) {
    return sql_int_init(ctx, *(int *)state, false);
}

static sql_node_t *sql_func_row_num(sql_ctx_t *ctx, sql_node_t *f) {
    return sql_int_init(ctx, 0, true);
}

static sql_ctx_spec_update_t *update_row_num(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    sql_ctx_spec_update_t *u = aml_pool_zalloc(ctx->pool, sizeof(*u));
    u->implementation = sql_func_row_num;
    u->return_type = SQL_TYPE_INT;
    return u;
}

static sql_ctx_spec_t row_number_spec = {
    .name = "ROW_NUMBER",
    .description = "Returns sequential row number within a partition.",
    .update = update_row_num,
    .is_aggregate = true,
    .state_size = sizeof(int),
    .agg_init = row_num_init,
    .agg_step = row_num_step,
    .agg_finalize = row_num_finalize,
    .is_volatile = true,
    .is_window_func = true
};

void sql_register_row_number(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &row_number_spec);
    sql_ctx_register_callback(ctx, sql_func_row_num, "row_number", "Returns sequential row number within a partition.");
}
