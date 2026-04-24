// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_node.h"

// Boolean Operators Implementation

// AND Implementation
static sql_node_t *sql_func_and(sql_ctx_t *ctx, sql_node_t *f) {
    bool result = true;
    for (size_t i = 0; i < f->num_parameters; i++) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (!child || child->is_null) {
            return sql_bool_init(ctx, false, true);
        }
        if (!child->value.bool_value) {
            result = false;
        }
    }
    return sql_bool_init(ctx, result, false);
}

// OR Implementation
static sql_node_t *sql_func_or(sql_ctx_t *ctx, sql_node_t *f) {
    bool result = false;
    for (size_t i = 0; i < f->num_parameters; i++) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (!child || child->is_null) {
            return sql_bool_init(ctx, false, true);
        }
        if (child->value.bool_value) {
            result = true;
        }
    }
    return sql_bool_init(ctx, result, false);
}

// NOT Implementation
static sql_node_t *sql_func_not(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 1) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, !child->value.bool_value, false);
}

// Specification Update Functions

// Update function for AND
static sql_ctx_spec_update_t *update_and_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = f->num_parameters;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, f->num_parameters * sizeof(sql_data_type_t));

    // All parameters should be of boolean type
    for (size_t i = 0; i < f->num_parameters; i++) {
        update->expected_data_types[i] = SQL_TYPE_BOOL;
    }

    update->implementation = sql_func_and;
    update->return_type = SQL_TYPE_BOOL;
    return update;
}

// Update function for OR
static sql_ctx_spec_update_t *update_or_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = f->num_parameters;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, f->num_parameters * sizeof(sql_data_type_t));

    // All parameters should be of boolean type
    for (size_t i = 0; i < f->num_parameters; i++) {
        update->expected_data_types[i] = SQL_TYPE_BOOL;
    }

    update->implementation = sql_func_or;
    update->return_type = SQL_TYPE_BOOL;
    return update;
}

// Update function for NOT
static sql_ctx_spec_update_t *update_not_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    if(f->num_parameters != 1) {
        return NULL;
    }
    update->num_parameters = 1; // NOT should have exactly one parameter
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, sizeof(sql_data_type_t));

    // Parameter should be of boolean type
    update->expected_data_types[0] = SQL_TYPE_BOOL;

    update->implementation = sql_func_not;
    update->return_type = SQL_TYPE_BOOL;
    return update;
}

// Function Specifications
static sql_ctx_spec_t and_function_spec = {
    .name = "AND",
    .description = "Logical AND operation.",
    .update = update_and_spec
};

static sql_ctx_spec_t or_function_spec = {
    .name = "OR",
    .description = "Logical OR operation.",
    .update = update_or_spec
};

static sql_ctx_spec_t not_function_spec = {
    .name = "NOT",
    .description = "Logical NOT operation.",
    .update = update_not_spec
};

// Registration Function
void sql_register_boolean(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &and_function_spec);
    sql_ctx_register_spec(ctx, &or_function_spec);
    sql_ctx_register_spec(ctx, &not_function_spec);

    // Optionally, register the direct implementations for debugging or manual invocation
    sql_ctx_register_callback(ctx, sql_func_and, "and", "Performs logical AND on boolean values.");
    sql_ctx_register_callback(ctx, sql_func_or, "or", "Performs logical OR on boolean values.");
    sql_ctx_register_callback(ctx, sql_func_not, "not", "Performs logical NOT on a boolean value.");
}
