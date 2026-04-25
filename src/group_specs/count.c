// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_node.h"
#include "sql-parser-library/sql_ctx.h"

// --- LIFECYCLE ---
static void count_init(void *state) {
    *(int *)state = 0;
}

static void count_step(sql_ctx_t *ctx, sql_node_t *f, void *state) {
    // Intercept the raw '*' token natively before evaluating!
    if (f->num_parameters == 0 ||
       (f->num_parameters == 1 && f->parameters[0]->token && strcmp(f->parameters[0]->token, "*") == 0)) {
        *(int *)state += 1;
    } else {
        // COUNT(column) - Only count non-null values
        sql_node_t *child = sql_eval(ctx, f->parameters[0]);
        if (child && !child->is_null) {
            *(int *)state += 1;
        }
    }
}

static sql_node_t *count_finalize(sql_ctx_t *ctx, sql_node_t *f, void *state) {
    return sql_int_init(ctx, *(int *)state, false);
}

// --- EVALUATION BRIDGE ---
static sql_node_t *sql_func_count(sql_ctx_t *ctx, sql_node_t *f) {
    if (ctx->current_agg_states) {
        return count_finalize(ctx, f, ctx->current_agg_states[f->agg_index]);
    }
    return sql_int_init(ctx, 0, false);
}

static sql_ctx_spec_update_t *update_count_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = f->num_parameters;
    update->parameters = f->parameters;

    // COUNT can take 0 or 1 parameters. We don't care about the type of the parameter.
    if (f->num_parameters > 0) {
        update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, f->num_parameters * sizeof(sql_data_type_t));
        for (size_t i = 0; i < f->num_parameters; i++) {
            update->expected_data_types[i] = SQL_TYPE_UNKNOWN;
        }
    }

    update->implementation = sql_func_count;
    update->return_type = SQL_TYPE_INT;
    return update;
}

static sql_ctx_spec_t count_spec = {
    .name = "COUNT",
    .description = "Calculates the number of rows.",
    .update = update_count_spec,

    .is_aggregate = true,
    .state_size = sizeof(int),
    .agg_init = count_init,
    .agg_step = count_step,
    .agg_finalize = count_finalize
};

void sql_register_count(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &count_spec);
    sql_ctx_register_callback(ctx, sql_func_count, "count", "Calculates the number of rows.");
}
