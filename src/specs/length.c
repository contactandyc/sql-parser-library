// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"
#include "a-memory-library/aml_pool.h"
#include <string.h>

// Implementations for LENGTH
static sql_node_t *sql_string_length(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 1) {
        sql_ctx_error(ctx, "LENGTH function requires exactly one parameter.");
        return sql_int_init(ctx, 0, true);
    }

    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_STRING) {
        return sql_int_init(ctx, 0, true); // Return NULL if input is NULL or not a string
    }

    int length = (int)strlen(child->value.string_value);
    return sql_int_init(ctx, length, false); // Return the length of the string
}

// Update function for LENGTH
static sql_ctx_spec_update_t *update_length_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 1) {
        sql_ctx_error(ctx, "LENGTH function requires exactly one parameter.");
        return NULL;
    }

    // Allocate and initialize the update structure
    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));

    update->num_parameters = f->num_parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, sizeof(sql_data_type_t));
    update->parameters = f->parameters;

    // Expect the parameter to be of type SQL_TYPE_STRING
    update->expected_data_types[0] = SQL_TYPE_STRING;

    // Assign the implementation and return type
    update->implementation = sql_string_length;
    update->return_type = SQL_TYPE_INT;

    return update;
}

// LENGTH function spec
sql_ctx_spec_t length_function_spec = {
    .name = "LENGTH",
    .description = "Returns the length of a string.",
    .update = update_length_spec
};

// Register LENGTH function
void sql_register_length(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &length_function_spec);

    sql_ctx_register_callback(ctx, sql_string_length, "string_length",
                              "Returns the length of a string.");
}
