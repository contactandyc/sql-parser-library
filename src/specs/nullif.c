// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"
#include <string.h>

static sql_node_t *sql_int_nullif(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);

    if (!left || left->is_null) return sql_int_init(ctx, 0, true);
    if (!right || right->is_null) return left;

    if (left->value.int_value == right->value.int_value) {
        return sql_int_init(ctx, 0, true);
    }
    return left;
}

static sql_node_t *sql_double_nullif(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);

    if (!left || left->is_null) return sql_double_init(ctx, 0.0, true);
    if (!right || right->is_null) return left;

    if (left->value.double_value == right->value.double_value) {
        return sql_double_init(ctx, 0.0, true);
    }
    return left;
}

static sql_node_t *sql_string_nullif(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);

    if (!left || left->is_null) return sql_string_init(ctx, NULL, true);
    if (!right || right->is_null) return left;

    if (strcasecmp(left->value.string_value, right->value.string_value) == 0) {
        return sql_string_init(ctx, NULL, true);
    }
    return left;
}

static sql_ctx_spec_update_t *update_nullif_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 2) {
        sql_ctx_error(ctx, "NULLIF requires exactly two parameters.");
        return NULL;
    }

    sql_data_type_t common_type = sql_determine_common_type(f->parameters[0]->data_type, f->parameters[1]->data_type);
    if (common_type == SQL_TYPE_UNKNOWN) {
        sql_ctx_error(ctx, "NULLIF parameters must have compatible types.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 2;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, 2 * sizeof(sql_data_type_t));

    // Both args coerced to the common type
    update->expected_data_types[0] = common_type;
    update->expected_data_types[1] = common_type;
    update->return_type = common_type;

    switch (common_type) {
        case SQL_TYPE_INT: update->implementation = sql_int_nullif; break;
        case SQL_TYPE_DOUBLE: update->implementation = sql_double_nullif; break;
        case SQL_TYPE_STRING: update->implementation = sql_string_nullif; break;
        default:
            sql_ctx_error(ctx, "Unsupported parameter type for NULLIF.");
            return NULL;
    }
    return update;
}

sql_ctx_spec_t nullif_spec = {
    .name = "NULLIF",
    .description = "Returns NULL if the two expressions are equal, otherwise returns the first.",
    .update = update_nullif_spec
};

void sql_register_nullif(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &nullif_spec);
    sql_ctx_register_callback(ctx, sql_int_nullif, "int_nullif", "NULLIF for ints");
    sql_ctx_register_callback(ctx, sql_double_nullif, "double_nullif", "NULLIF for doubles");
    sql_ctx_register_callback(ctx, sql_string_nullif, "string_nullif", "NULLIF for strings");
}
