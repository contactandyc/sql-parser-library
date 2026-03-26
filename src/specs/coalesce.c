// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"

// Implementations for each data type
static sql_node_t *sql_bool_coalesce(sql_ctx_t *ctx, sql_node_t *f) {
    for (size_t i = 0; i < f->num_parameters; i++) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (child && !child->is_null) {
            return sql_bool_init(ctx, child->value.bool_value, false);
        }
    }
    return sql_bool_init(ctx, false, true); // Return NULL if all values are NULL
}

static sql_node_t *sql_string_coalesce(sql_ctx_t *ctx, sql_node_t *f) {
    for (size_t i = 0; i < f->num_parameters; i++) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (child && !child->is_null) {
            return sql_string_init(ctx, child->value.string_value, false);
        }
    }
    return sql_string_init(ctx, NULL, true); // Return NULL if all values are NULL
}

static sql_node_t *sql_datetime_coalesce(sql_ctx_t *ctx, sql_node_t *f) {
    for (size_t i = 0; i < f->num_parameters; i++) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (child && !child->is_null) {
            return sql_datetime_init(ctx, child->value.epoch, false);
        }
    }
    return sql_datetime_init(ctx, 0, true); // Return NULL if all values are NULL
}

static sql_node_t *sql_int_coalesce(sql_ctx_t *ctx, sql_node_t *f) {
    for (size_t i = 0; i < f->num_parameters; i++) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (child && !child->is_null) {
            return sql_int_init(ctx, child->value.int_value, false);
        }
    }
    return sql_int_init(ctx, 0, true); // Return NULL if all values are NULL
}

static sql_node_t *sql_double_coalesce(sql_ctx_t *ctx, sql_node_t *f) {
    for (size_t i = 0; i < f->num_parameters; i++) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (child && !child->is_null) {
            return sql_double_init(ctx, child->value.double_value, false);
        }
    }
    return sql_double_init(ctx, 0.0, true); // Return NULL if all values are NULL
}

// Update function for COALESCE
static sql_ctx_spec_update_t *update_coalesce_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters < 1) {
        sql_ctx_error(ctx, "COALESCE function requires at least one parameter.");
        return NULL;
    }

    // Determine the common type
    sql_data_type_t common_type = f->parameters[0]->data_type;

    for (size_t i = 1; i < f->num_parameters; i++) {
        sql_data_type_t param_type = f->parameters[i]->data_type;
        if (param_type != common_type) {
            if ((common_type == SQL_TYPE_INT && param_type == SQL_TYPE_DOUBLE) ||
                (common_type == SQL_TYPE_DOUBLE && param_type == SQL_TYPE_INT)) {
                common_type = SQL_TYPE_DOUBLE; // Promote to double for mixed int/double
            } else {
                sql_ctx_error(ctx, "COALESCE function parameters must have compatible types.");
                return NULL;
            }
        }
    }

    // Allocate and initialize the update structure
    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));

    update->num_parameters = f->num_parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, f->num_parameters * sizeof(sql_data_type_t));
    update->parameters = f->parameters;

    for (size_t i = 0; i < f->num_parameters; i++) {
        update->expected_data_types[i] = common_type;
    }

    // Assign the correct implementation and return type
    switch (common_type) {
        case SQL_TYPE_BOOL:
            update->implementation = sql_bool_coalesce;
            update->return_type = SQL_TYPE_BOOL;
            break;
        case SQL_TYPE_STRING:
            update->implementation = sql_string_coalesce;
            update->return_type = SQL_TYPE_STRING;
            break;
        case SQL_TYPE_DATETIME:
            update->implementation = sql_datetime_coalesce;
            update->return_type = SQL_TYPE_DATETIME;
            break;
        case SQL_TYPE_INT:
            update->implementation = sql_int_coalesce;
            update->return_type = SQL_TYPE_INT;
            break;
        case SQL_TYPE_DOUBLE:
            update->implementation = sql_double_coalesce;
            update->return_type = SQL_TYPE_DOUBLE;
            break;
        default:
            sql_ctx_error(ctx, "Unsupported parameter type for COALESCE function.");
            return NULL;
    }

    return update;
}

// COALESCE function spec
sql_ctx_spec_t coalesce_function_spec = {
    .name = "COALESCE",
    .description = "Returns the first non-NULL value from the list of arguments.",
    .update = update_coalesce_spec
};

// Register COALESCE function
void sql_register_coalesce(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &coalesce_function_spec);

    sql_ctx_register_callback(ctx, sql_bool_coalesce, "bool_coalesce",
                              "Returns the first non-NULL boolean value.");
    sql_ctx_register_callback(ctx, sql_string_coalesce, "string_coalesce",
                              "Returns the first non-NULL string value.");
    sql_ctx_register_callback(ctx, sql_datetime_coalesce, "datetime_coalesce",
                              "Returns the first non-NULL datetime value.");
    sql_ctx_register_callback(ctx, sql_int_coalesce, "int_coalesce",
                              "Returns the first non-NULL integer value.");
    sql_ctx_register_callback(ctx, sql_double_coalesce, "double_coalesce",
                              "Returns the first non-NULL double value.");
}
