// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_node.h"

// Implementation for IS TRUE
static sql_node_t *sql_is_true(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 1) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child) {
        return sql_bool_init(ctx, false, true);
    }
    // IS TRUE: only true if the value is non-null and its boolean value is true.
    bool result = (!child->is_null && child->value.bool_value);
    return sql_bool_init(ctx, result, false);
}

// Implementation for IS NOT TRUE
static sql_node_t *sql_is_not_true(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 1) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child) {
        return sql_bool_init(ctx, false, true);
    }
    // IS NOT TRUE: true if the value is either null or explicitly false.
    bool result = (child->is_null || !child->value.bool_value);
    return sql_bool_init(ctx, result, false);
}

// Implementation for IS FALSE
static sql_node_t *sql_is_false(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 1) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child) {
        return sql_bool_init(ctx, false, true);
    }
    // IS FALSE: only true if the value is non-null and its boolean value is false.
    bool result = (!child->is_null && !child->value.bool_value);
    return sql_bool_init(ctx, result, false);
}

// Implementation for IS NOT FALSE
static sql_node_t *sql_is_not_false(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 1) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child) {
        return sql_bool_init(ctx, false, true);
    }
    // IS NOT FALSE: true if the value is either null or explicitly true.
    bool result = (child->is_null || child->value.bool_value);
    return sql_bool_init(ctx, result, false);
}

// Update function for IS TRUE
static sql_ctx_spec_update_t *update_is_true_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 1) {
        sql_ctx_error(ctx, "IS TRUE requires exactly one parameter.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 1;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, sizeof(sql_data_type_t));

    // Accept any data type for IS TRUE
    update->expected_data_types[0] = update->parameters[0]->data_type;

    update->implementation = sql_is_true;
    update->return_type = SQL_TYPE_BOOL;

    return update;
}

// Update function for IS NOT TRUE
static sql_ctx_spec_update_t *update_is_not_true_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 1) {
        sql_ctx_error(ctx, "IS NOT TRUE requires exactly one parameter.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 1;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, sizeof(sql_data_type_t));

    // Accept any data type for IS NOT TRUE
    update->expected_data_types[0] = update->parameters[0]->data_type;

    update->implementation = sql_is_not_true;
    update->return_type = SQL_TYPE_BOOL;

    return update;
}

// Update function for IS FALSE
static sql_ctx_spec_update_t *update_is_false_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 1) {
        sql_ctx_error(ctx, "IS FALSE requires exactly one parameter.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 1;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, sizeof(sql_data_type_t));

    // Accept any data type for IS FALSE
    update->expected_data_types[0] = update->parameters[0]->data_type;

    update->implementation = sql_is_false;
    update->return_type = SQL_TYPE_BOOL;

    return update;
}

// Update function for IS NOT FALSE
static sql_ctx_spec_update_t *update_is_not_false_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 1) {
        sql_ctx_error(ctx, "IS NOT FALSE requires exactly one parameter.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 1;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, sizeof(sql_data_type_t));

    // Accept any data type for IS NOT FALSE
    update->expected_data_types[0] = update->parameters[0]->data_type;

    update->implementation = sql_is_not_false;
    update->return_type = SQL_TYPE_BOOL;

    return update;
}

// Specifications for IS TRUE, IS NOT TRUE, IS FALSE, and IS NOT FALSE
static sql_ctx_spec_t is_true_function_spec = {
    .name = "IS TRUE",
    .description = "Checks if a value is TRUE.",
    .update = update_is_true_spec
};

static sql_ctx_spec_t is_not_true_function_spec = {
    .name = "IS NOT TRUE",
    .description = "Checks if a value is NOT TRUE.",
    .update = update_is_not_true_spec
};

static sql_ctx_spec_t is_false_function_spec = {
    .name = "IS FALSE",
    .description = "Checks if a value is FALSE.",
    .update = update_is_false_spec
};

static sql_ctx_spec_t is_not_false_function_spec = {
    .name = "IS NOT FALSE",
    .description = "Checks if a value is NOT FALSE.",
    .update = update_is_not_false_spec
};

// Registration function for IS TRUE, IS NOT TRUE, IS FALSE, and IS NOT FALSE
void sql_register_is_boolean(sql_ctx_t *ctx) {
    // Register the specifications
    sql_ctx_register_spec(ctx, &is_true_function_spec);
    sql_ctx_register_spec(ctx, &is_not_true_function_spec);
    sql_ctx_register_spec(ctx, &is_false_function_spec);
    sql_ctx_register_spec(ctx, &is_not_false_function_spec);

    // Register direct implementations for debugging or manual invocation
    sql_ctx_register_callback(ctx, sql_is_true, "is_true", "Check if a value is TRUE.");
    sql_ctx_register_callback(ctx, sql_is_not_true, "is_not_true", "Check if a value is NOT TRUE.");
    sql_ctx_register_callback(ctx, sql_is_false, "is_false", "Check if a value is FALSE.");
    sql_ctx_register_callback(ctx, sql_is_not_false, "is_not_false", "Check if a value is NOT FALSE.");
}
