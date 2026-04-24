// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"
#include "a-memory-library/aml_pool.h"
#include <string.h>

// Implementations for CONCAT
static sql_node_t *sql_string_concat(sql_ctx_t *ctx, sql_node_t *f) {
    aml_pool_t *pool = ctx->pool;

    // Calculate the total length of the concatenated string
    size_t total_length = 0;
    for (size_t i = 0; i < f->num_parameters; i++) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (child && !child->is_null && child->data_type == SQL_TYPE_STRING) {
            total_length += strlen(child->value.string_value);
        }
    }

    if (total_length == 0) {
        return sql_string_init(ctx, NULL, true); // Return NULL if all parameters are NULL
    }

    // Allocate memory for the concatenated string
    char *result = (char *)aml_pool_alloc(pool, total_length + 1);
    result[0] = '\0'; // Initialize the result string

    // Concatenate the strings
    for (size_t i = 0; i < f->num_parameters; i++) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (child && !child->is_null && child->data_type == SQL_TYPE_STRING) {
            strcat(result, child->value.string_value);
        }
    }

    return sql_string_init(ctx, result, false); // Return the concatenated string
}

// Update function for CONCAT
static sql_ctx_spec_update_t *update_concat_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters < 1) {
        sql_ctx_error(ctx, "CONCAT function requires at least one parameter.");
        return NULL;
    }

    // Allocate and initialize the update structure
    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));

    update->num_parameters = f->num_parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, f->num_parameters * sizeof(sql_data_type_t));
    update->parameters = f->parameters;

    // Set all parameters to be of type SQL_TYPE_STRING
    for (size_t i = 0; i < f->num_parameters; i++) {
        update->expected_data_types[i] = SQL_TYPE_STRING;
    }

    // Assign the implementation and return type
    update->implementation = sql_string_concat;
    update->return_type = SQL_TYPE_STRING;

    return update;
}

static sql_ctx_spec_t concat_op_spec = {
    .name = "||",
    .description = "String concatenation operator",
    .update = update_concat_spec
};

// CONCAT function spec
static sql_ctx_spec_t concat_function_spec = {
    .name = "CONCAT",
    .description = "Concatenates multiple string values into a single string.",
    .update = update_concat_spec
};

// Register CONCAT function
void sql_register_concat(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &concat_function_spec);
    sql_ctx_register_spec(ctx, &concat_op_spec);
    sql_ctx_register_callback(ctx, sql_string_concat, "string_concat",
                              "Concatenates multiple string values into a single string.");
}
