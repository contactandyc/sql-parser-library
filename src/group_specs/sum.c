// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_node.h"
#include "sql-parser-library/sql_ctx.h"

// --- LIFECYCLE ---
static void sum_init(void *state) {
    *(double *)state = 0.0;
}

static void sum_step(sql_ctx_t *ctx, sql_node_t *f, void *state) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (child && !child->is_null) {
        *(double *)state += child->value.double_value;
    }
}

static sql_node_t *sum_finalize(sql_ctx_t *ctx, sql_node_t *f, void *state) {
    return sql_double_init(ctx, *(double *)state, false);
}

// --- EVALUATION BRIDGE ---
static sql_node_t *sql_func_sum(sql_ctx_t *ctx, sql_node_t *f) {
    // If we are in the projection phase (active states exist), grab the accumulated value!
    if (ctx->current_agg_states) {
        return sum_finalize(ctx, f, ctx->current_agg_states[f->agg_index]);
    }
    return sql_double_init(ctx, 0.0, true); // Fallback for uninitialized state
}

static sql_ctx_spec_update_t *update_sum_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters < 1) {
        sql_ctx_error(ctx, "SUM requires at least one parameter.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = f->num_parameters;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, f->num_parameters * sizeof(sql_data_type_t));

    for (size_t i = 0; i < f->num_parameters; i++) {
        update->expected_data_types[i] = SQL_TYPE_DOUBLE;
    }

    update->implementation = sql_func_sum;
    update->return_type = SQL_TYPE_DOUBLE;
    return update;
}

static sql_ctx_spec_t sum_spec = {
    .name = "SUM",
    .description = "Calculates the sum of numeric values.",
    .update = update_sum_spec,

    // --- ENABLE AGGREGATION ENGINE ---
    .is_aggregate = true,
    .state_size = sizeof(double),
    .agg_init = sum_init,
    .agg_step = sum_step,
    .agg_finalize = sum_finalize
};

void sql_register_sum(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &sum_spec);
    sql_ctx_register_callback(ctx, sql_func_sum, "sum", "Calculates the sum of numeric values.");
}
