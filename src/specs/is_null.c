// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_node.h"

// Implementation for IS NULL
static sql_node_t *sql_is_null(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 1) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, child->is_null, false);
}

// Implementation for IS NOT NULL
static sql_node_t *sql_is_not_null(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 1) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, !child->is_null, false);
}

// Update function for IS NULL
static sql_ctx_spec_update_t *update_is_null_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 1) {
        sql_ctx_error(ctx, "IS NULL requires exactly one parameter.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 1;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, sizeof(sql_data_type_t));

    // Any data type is acceptable for IS NULL
    update->expected_data_types[0] = update->parameters[0]->data_type;

    update->implementation = sql_is_null;
    update->return_type = SQL_TYPE_BOOL;

    return update;
}

// Update function for IS NOT NULL
static sql_ctx_spec_update_t *update_is_not_null_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 1) {
        sql_ctx_error(ctx, "IS NOT NULL requires exactly one parameter.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 1;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, sizeof(sql_data_type_t));

    // Any data type is acceptable for IS NOT NULL
    update->expected_data_types[0] = update->parameters[0]->data_type;

    update->implementation = sql_is_not_null;
    update->return_type = SQL_TYPE_BOOL;

    return update;
}

// Specifications for IS NULL and IS NOT NULL
static sql_ctx_spec_t is_null_function_spec = {
    .name = "IS NULL",
    .description = "Checks if a value is NULL.",
    .update = update_is_null_spec
};

static sql_ctx_spec_t is_not_null_function_spec = {
    .name = "IS NOT NULL",
    .description = "Checks if a value is NOT NULL.",
    .update = update_is_not_null_spec
};

// Registration function for IS NULL and IS NOT NULL
void sql_register_is_null(sql_ctx_t *ctx) {
    // Register the specifications
    sql_ctx_register_spec(ctx, &is_null_function_spec);
    sql_ctx_register_spec(ctx, &is_not_null_function_spec);

    // Register direct implementations for debugging or manual invocation
    sql_ctx_register_callback(ctx, sql_is_null, "is_null", "Check if a value is NULL.");
    sql_ctx_register_callback(ctx, sql_is_not_null, "is_not_null", "Check if a value is NOT NULL.");
}
