// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_node.h"
#include <ctype.h>

static sql_node_t *sql_func_lower(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_string_init(ctx, NULL, true);
    }

    const char *input = child->value.string_value;
    char *result = aml_pool_strdup(ctx->pool, input);

    for (char *p = result; *p; ++p) {
        *p = tolower(*p);
    }

    return sql_string_init(ctx, result, false);
}

static sql_node_t *sql_func_upper(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_string_init(ctx, NULL, true);
    }

    const char *input = child->value.string_value;
    char *result = aml_pool_strdup(ctx->pool, input);

    for (char *p = result; *p; ++p) {
        *p = toupper(*p);
    }

    return sql_string_init(ctx, result, false);
}

static sql_ctx_spec_update_t *update_lower_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 1) {
        sql_ctx_error(ctx, "LOWER requires exactly one parameter.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 1;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, sizeof(sql_data_type_t));
    update->expected_data_types[0] = SQL_TYPE_STRING;

    if (f->parameters[0]->data_type != SQL_TYPE_STRING) {
        sql_ctx_error(ctx, "LOWER only supports STRING data type.");
        return NULL;
    }

    update->return_type = SQL_TYPE_STRING;
    update->implementation = sql_func_lower;
    return update;
}

static sql_ctx_spec_t lower_spec = {
    .name = "LOWER",
    .description = "Converts a string to lowercase.",
    .update = update_lower_spec
};

static sql_ctx_spec_update_t *update_upper_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 1) {
        sql_ctx_error(ctx, "UPPER requires exactly one parameter.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 1;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, sizeof(sql_data_type_t));
    update->expected_data_types[0] = SQL_TYPE_STRING;

    if (f->parameters[0]->data_type != SQL_TYPE_STRING) {
        sql_ctx_error(ctx, "UPPER only supports STRING data type.");
        return NULL;
    }

    update->return_type = SQL_TYPE_STRING;
    update->implementation = sql_func_upper;
    return update;
}

static sql_ctx_spec_t upper_spec = {
    .name = "UPPER",
    .description = "Converts a string to uppercase.",
    .update = update_upper_spec
};

void sql_register_lower_upper(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &lower_spec);
    sql_ctx_register_spec(ctx, &upper_spec);

    sql_ctx_register_callback(ctx, sql_func_lower, "lower", "Converts a string to lowercase.");
    sql_ctx_register_callback(ctx, sql_func_upper, "upper", "Converts a string to uppercase.");
}
