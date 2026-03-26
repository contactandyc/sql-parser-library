// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"

static sql_node_t *sql_func_avg(sql_ctx_t *ctx, sql_node_t *f) {
    double result = 0.0;
    for (size_t i = 0; i < f->num_parameters; i++) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (!child || child->is_null) {
            return sql_double_init(ctx, 0, true);
        }
        result += child->value.double_value;
    }
    return sql_double_init(ctx, result / f->num_parameters, false);
}

static sql_ctx_spec_update_t *update_avg_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters < 1) {
        sql_ctx_error(ctx, "AVG requires at least one parameter.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = f->num_parameters;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, f->num_parameters * sizeof(sql_data_type_t));

    for (size_t i = 0; i < f->num_parameters; i++) {
        if (f->parameters[i]->data_type != SQL_TYPE_DOUBLE && f->parameters[i]->data_type != SQL_TYPE_INT) {
            sql_ctx_error(ctx, "AVG only supports numeric data types (INT, DOUBLE).");
            return NULL;
        }
        update->expected_data_types[i] = SQL_TYPE_DOUBLE;
    }

    update->return_type = SQL_TYPE_DOUBLE;
    update->implementation = sql_func_avg;
    return update;
}

sql_ctx_spec_t avg_spec = {
    .name = "AVG",
    .description = "Calculates the average of numeric values.",
    .update = update_avg_spec
};

void sql_register_avg(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &avg_spec);

    sql_ctx_register_callback(ctx, sql_func_avg, "avg", "Calculates the average of numeric values.");
}
