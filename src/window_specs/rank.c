// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_node.h"
#include "sql-parser-library/sql_ctx.h"

typedef struct {
    int current_rank;
    int rows_in_partition;
} rank_state_t;

static void rank_init(void *state) {
    rank_state_t *s = state;
    s->current_rank = 1;
    s->rows_in_partition = 0;
}

static void rank_step(sql_ctx_t *ctx, sql_node_t *f, void *state) {
    rank_state_t *s = state;
    s->rows_in_partition++;
}

static sql_node_t *rank_finalize(sql_ctx_t *ctx, sql_node_t *f, void *state) {
    rank_state_t *s = state;
    return sql_int_init(ctx, s->current_rank, false);
}

static sql_node_t *sql_func_rank(sql_ctx_t *ctx, sql_node_t *f) {
    return sql_int_init(ctx, 0, true);
}

static sql_ctx_spec_update_t *update_rank(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    sql_ctx_spec_update_t *u = aml_pool_zalloc(ctx->pool, sizeof(*u));
    u->implementation = sql_func_rank;
    u->return_type = SQL_TYPE_INT;
    return u;
}

sql_ctx_spec_t rank_spec = {
    .name = "RANK",
    .description = "Returns rank of the current row with gaps.",
    .update = update_rank,
    .is_aggregate = true,
    .state_size = sizeof(rank_state_t),
    .agg_init = rank_init,
    .agg_step = rank_step,
    .agg_finalize = rank_finalize,
    .is_volatile = true,
    .is_window_func = true
};

void sql_register_rank(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &rank_spec);
    sql_ctx_register_callback(ctx, sql_func_rank, "rank", "Returns rank of the current row with gaps.");
}
