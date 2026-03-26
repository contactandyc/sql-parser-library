// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_node.h"
#include "sql-parser-library/sql_ctx.h"

static sql_node_t *sql_func_substr_two_params(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *str_node = sql_eval(ctx, f->parameters[0]);
    sql_node_t *start_node = sql_eval(ctx, f->parameters[1]);

    if (!str_node || str_node->is_null || !start_node || start_node->is_null) {
        return sql_string_init(ctx, NULL, true);
    }

    const char *input_str = str_node->value.string_value;
    int start_pos = start_node->value.int_value - 1; // Convert to 0-based index

    if (start_pos < 0 || start_pos >= (int)strlen(input_str)) {
        return sql_string_init(ctx, NULL, true); // Invalid indices return NULL
    }

    char *result = aml_pool_strdup(ctx->pool, input_str + start_pos);
    return sql_string_init(ctx, result, false);
}

static sql_node_t *sql_func_substr_three_params(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *str_node = sql_eval(ctx, f->parameters[0]);
    sql_node_t *start_node = sql_eval(ctx, f->parameters[1]);
    sql_node_t *length_node = sql_eval(ctx, f->parameters[2]);

    if (!str_node || str_node->is_null || !start_node || start_node->is_null || !length_node || length_node->is_null) {
        return sql_string_init(ctx, NULL, true);
    }

    const char *input_str = str_node->value.string_value;
    int start_pos = start_node->value.int_value - 1; // Convert to 0-based index
    int length = length_node->value.int_value;

    if (start_pos < 0 || start_pos >= (int)strlen(input_str) || length < 0) {
        return sql_string_init(ctx, NULL, true); // Invalid indices return NULL
    }

    char *result = aml_pool_alloc(ctx->pool, length + 1);
    strncpy(result, input_str + start_pos, length);
    result[length] = '\0'; // Ensure null-termination
    return sql_string_init(ctx, result, false);
}

static sql_ctx_spec_update_t *update_substr_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters < 2 || f->num_parameters > 3) {
        sql_ctx_error(ctx, "SUBSTR requires either two or three parameters.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = f->num_parameters;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, f->num_parameters * sizeof(sql_data_type_t));
    update->expected_data_types[0] = SQL_TYPE_STRING; // First parameter must be a string
    update->expected_data_types[1] = SQL_TYPE_INT;    // Second parameter must be an integer

    if (f->num_parameters == 2) {
        update->implementation = sql_func_substr_two_params;
    } else {
        update->expected_data_types[2] = SQL_TYPE_INT; // Third parameter must also be an integer
        update->implementation = sql_func_substr_three_params;
    }

    update->return_type = SQL_TYPE_STRING; // SUBSTR always returns a string
    return update;
}

sql_ctx_spec_t substr_spec = {
    .name = "SUBSTR",
    .description = "Extracts a substring from a string starting at a given position and optionally up to a given length.",
    .update = update_substr_spec
};

sql_ctx_spec_t substring_spec = {
    .name = "SUBSTRING",
    .description = "Extracts a substring from a string starting at a given position and optionally up to a given length.",
    .update = update_substr_spec
};


void sql_register_substr(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &substr_spec);
    sql_ctx_register_spec(ctx, &substring_spec);

    // Optionally, register the two and three parameter implementations for manual invocation
    sql_ctx_register_callback(ctx, sql_func_substr_two_params, "substr_two_params", "Extract substring with two parameters.");
    sql_ctx_register_callback(ctx, sql_func_substr_three_params, "substr_three_params", "Extract substring with three parameters.");
}
