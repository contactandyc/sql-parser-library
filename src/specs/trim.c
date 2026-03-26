// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"
#include <strings.h>

static sql_node_t *sql_trim(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_string_init(ctx, NULL, true);
    }
    char *value = (char *)child->value.string_value;
    if(!value) {
        return sql_string_init(ctx, NULL, true);
    }
    // Trim leading spaces
    while (*value == ' ') {
        value++;
    }
    // Trim trailing spaces
    char *end = value + strlen(value) - 1;
    char *start_end = end;
    while (end > value && *end == ' ') {
        end--;
    }
    if(end < start_end) {
        size_t index = end - value + 1;
        value = aml_pool_strdup(ctx->pool, value);
        value[index] = '\0';
    }
    return sql_string_init(ctx, value, false);
}

static sql_node_t *sql_rtrim(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_string_init(ctx, NULL, true);
    }
    char *value = (char *)child->value.string_value;
    if (!value) {
        return sql_string_init(ctx, NULL, true);
    }
    // Trim trailing spaces
    char *end = value + strlen(value) - 1;
    char *start_end = end;
    while (end > value && *end == ' ') {
        end--;
    }
    if (end < start_end) {
        size_t index = end - value + 1;
        value = aml_pool_strdup(ctx->pool, value);
        value[index] = '\0';
    }
    return sql_string_init(ctx, value, false);
}

static sql_node_t *sql_ltrim(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_string_init(ctx, NULL, true);
    }
    char *value = (char *)child->value.string_value;
    if (!value) {
        return sql_string_init(ctx, NULL, true);
    }
    // Trim leading spaces
    while (*value == ' ') {
        value++;
    }
    value = aml_pool_strdup(ctx->pool, value);
    return sql_string_init(ctx, value, false);
}

static sql_ctx_spec_update_t *update_trim_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 1) {
        sql_ctx_error(ctx, "%s requires exactly one parameter.", spec->name);
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 1;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, sizeof(sql_data_type_t));
    update->expected_data_types[0] = SQL_TYPE_STRING;

    if (strcasecmp(spec->name, "TRIM") == 0) {
        update->implementation = sql_trim;
    } else if (strcasecmp(spec->name, "RTRIM") == 0) {
        update->implementation = sql_rtrim;
    } else if (strcasecmp(spec->name, "LTRIM") == 0) {
        update->implementation = sql_ltrim;
    } else {
        sql_ctx_error(ctx, "Unknown trim function: %s", spec->name);
        return NULL;
    }

    update->return_type = SQL_TYPE_STRING;
    return update;
}

sql_ctx_spec_t trim_spec = {
    .name = "TRIM",
    .description = "Removes leading and trailing spaces from a string.",
    .update = update_trim_spec
};

sql_ctx_spec_t rtrim_spec = {
    .name = "RTRIM",
    .description = "Removes trailing spaces from a string.",
    .update = update_trim_spec
};

sql_ctx_spec_t ltrim_spec = {
    .name = "LTRIM",
    .description = "Removes leading spaces from a string.",
    .update = update_trim_spec
};

void sql_register_trim(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &trim_spec);
    sql_ctx_register_spec(ctx, &rtrim_spec);
    sql_ctx_register_spec(ctx, &ltrim_spec);

    // Optionally register the implementations for debugging or manual invocation
    sql_ctx_register_callback(ctx, sql_trim, "trim", "Removes leading and trailing spaces.");
    sql_ctx_register_callback(ctx, sql_rtrim, "rtrim", "Removes trailing spaces.");
    sql_ctx_register_callback(ctx, sql_ltrim, "ltrim", "Removes leading spaces.");
}
