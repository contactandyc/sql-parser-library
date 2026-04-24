// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_node.h"
#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/date_utils.h"

static sql_node_t *sql_int_between(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 3) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *value = sql_eval(ctx, f->parameters[0]);
    sql_node_t *left = sql_eval(ctx, f->parameters[1]);
    sql_node_t *right = sql_eval(ctx, f->parameters[2]);
    if (!value || !left || !right || value->is_null || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, left->value.int_value <= value->value.int_value && value->value.int_value <= right->value.int_value, false);
}

static sql_node_t *sql_double_between(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 3) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *value = sql_eval(ctx, f->parameters[0]);
    sql_node_t *left = sql_eval(ctx, f->parameters[1]);
    sql_node_t *right = sql_eval(ctx, f->parameters[2]);
    if (!value || !left || !right || value->is_null || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, left->value.double_value <= value->value.double_value && value->value.double_value <= right->value.double_value, false);
}

static sql_node_t *sql_string_between(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 3) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *value = sql_eval(ctx, f->parameters[0]);
    sql_node_t *left = sql_eval(ctx, f->parameters[1]);
    sql_node_t *right = sql_eval(ctx, f->parameters[2]);
    if (!value || !left || !right || value->is_null || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, strcasecmp(left->value.string_value, value->value.string_value) <= 0 && strcasecmp(value->value.string_value, right->value.string_value) <= 0, false);
}

static sql_node_t *sql_datetime_between(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 3) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *value = sql_eval(ctx, f->parameters[0]);
    sql_node_t *left = sql_eval(ctx, f->parameters[1]);
    sql_node_t *right = sql_eval(ctx, f->parameters[2]);
    if (!value || !left || !right || value->is_null || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }

    return sql_bool_init(ctx, left->value.epoch <= value->value.epoch && value->value.epoch <= right->value.epoch, false);
}

static sql_node_t *sql_int_not_between(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *result = sql_int_between(ctx, f);
    if (!result) {
        return sql_bool_init(ctx, false, true);
    }
    result->value.bool_value = !result->value.bool_value;
    return result;
}

static sql_node_t *sql_double_not_between(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *result = sql_double_between(ctx, f);
    if (!result) {
        return sql_bool_init(ctx, false, true);
    }
    result->value.bool_value = !result->value.bool_value;
    return result;
}

static sql_node_t *sql_string_not_between(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *result = sql_string_between(ctx, f);
    if (!result) {
        return sql_bool_init(ctx, false, true);
    }
    result->value.bool_value = !result->value.bool_value;
    return result;
}

static sql_node_t *sql_datetime_not_between(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *result = sql_datetime_between(ctx, f);
    if (!result) {
        return sql_bool_init(ctx, false, true);
    }
    result->value.bool_value = !result->value.bool_value;
    return result;
}

// Updated function to handle implicit type conversion
static sql_ctx_spec_update_t *update_between_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 3) {
        sql_ctx_error(ctx, "BETWEEN requires exactly three parameters.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 3;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, 3 * sizeof(sql_data_type_t));

    // Determine the highest precision type for promotion
    sql_data_type_t common_type = sql_determine_common_type(
        sql_determine_common_type(f->parameters[0]->data_type, f->parameters[1]->data_type),
        f->parameters[2]->data_type
    );

    // Check for invalid type combinations
    if (common_type == SQL_TYPE_UNKNOWN) {
        sql_ctx_error(ctx, "BETWEEN only supports string, numeric, and datetime types.");
        return NULL;
    }

    // Convert all parameters to the highest precision type
    for (size_t i = 0; i < 3; i++) {
        update->expected_data_types[i] = common_type;
        if (f->parameters[i]->data_type != common_type) {
            f->parameters[i] = sql_convert(ctx, f->parameters[i], common_type);
        }
    }

    // Assign implementation based on common type
    switch (common_type) {
        case SQL_TYPE_INT:
            update->implementation = sql_int_between;
            update->return_type = SQL_TYPE_BOOL;
            break;
        case SQL_TYPE_DOUBLE:
            update->implementation = sql_double_between;
            update->return_type = SQL_TYPE_BOOL;
            break;
        case SQL_TYPE_DATETIME:
            update->implementation = sql_datetime_between;
            update->return_type = SQL_TYPE_BOOL;
            break;
        case SQL_TYPE_STRING:
            update->implementation = sql_string_between;
            update->return_type = SQL_TYPE_BOOL;
            break;
        default:
            sql_ctx_error(ctx, "BETWEEN is not supported for this type.");
            return NULL;
    }

    return update;
}

// Update function for NOT BETWEEN
static sql_ctx_spec_update_t *update_not_between_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 3) {
        sql_ctx_error(ctx, "NOT BETWEEN requires exactly three parameters.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 3;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, 3 * sizeof(sql_data_type_t));

    // Determine the highest precision type for promotion
    sql_data_type_t common_type = sql_determine_common_type(
        sql_determine_common_type(f->parameters[0]->data_type, f->parameters[1]->data_type),
        f->parameters[2]->data_type
    );

    // Check for invalid type combinations
    if (common_type == SQL_TYPE_UNKNOWN) {
        sql_ctx_error(ctx, "NOT BETWEEN only supports string, numeric, and datetime types.");
        return NULL;
    }

    // Convert all parameters to the highest precision type
    for (size_t i = 0; i < 3; i++) {
        update->expected_data_types[i] = common_type;
        if (f->parameters[i]->data_type != common_type) {
            f->parameters[i] = sql_convert(ctx, f->parameters[i], common_type);
        }
    }

    // Assign implementation based on common type
    switch (common_type) {
        case SQL_TYPE_INT:
            update->implementation = sql_int_not_between;
            update->return_type = SQL_TYPE_BOOL;
            break;
        case SQL_TYPE_DOUBLE:
            update->implementation = sql_double_not_between;
            update->return_type = SQL_TYPE_BOOL;
            break;
        case SQL_TYPE_STRING:
            update->implementation = sql_string_not_between;
            update->return_type = SQL_TYPE_BOOL;
            break;
        case SQL_TYPE_DATETIME:
            update->implementation = sql_datetime_not_between;
            update->return_type = SQL_TYPE_BOOL;
            break;
        default:
            sql_ctx_error(ctx, "NOT BETWEEN is not supported for this type.");
            return NULL;
    }

    return update;
}

// Specifications for BETWEEN and NOT BETWEEN
static sql_ctx_spec_t between_function_spec = {
    .name = "BETWEEN",
    .description = "Checks if a value is between two values.",
    .update = update_between_spec
};

static sql_ctx_spec_t not_between_function_spec = {
    .name = "NOT BETWEEN",
    .description = "Checks if a value is not between two values.",
    .update = update_not_between_spec
};

// Registration function
void sql_register_between(sql_ctx_t *ctx) {
    // Register the specifications
    sql_ctx_register_spec(ctx, &between_function_spec);
    sql_ctx_register_spec(ctx, &not_between_function_spec);

    // Register direct implementations for debugging or manual invocation
    sql_ctx_register_callback(ctx, sql_int_between, "int_between", "Check if value is between two integers");
    sql_ctx_register_callback(ctx, sql_double_between, "double_between", "Check if value is between two doubles");
    sql_ctx_register_callback(ctx, sql_string_between, "string_between", "Check if value is between two strings");
    sql_ctx_register_callback(ctx, sql_datetime_between, "datetime_between", "Check if value is between two datetimes");

    sql_ctx_register_callback(ctx, sql_int_not_between, "int_not_between", "Check if value is not between two integers");
    sql_ctx_register_callback(ctx, sql_double_not_between, "double_not_between", "Check if value is not between two doubles");
    sql_ctx_register_callback(ctx, sql_string_not_between, "string_not_between", "Check if value is not between two strings");
    sql_ctx_register_callback(ctx, sql_datetime_not_between, "datetime_not_between", "Check if value is not between two datetimes");
}
