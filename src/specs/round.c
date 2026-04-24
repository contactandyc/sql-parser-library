// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"
#include <math.h>

static sql_node_t *sql_func_round(sql_ctx_t *ctx, sql_node_t *f) {
    // Evaluate the parameter
    sql_node_t *value_node = sql_eval(ctx, f->parameters[0]);
    if (!value_node || value_node->is_null) {
        return sql_double_init(ctx, 0, true);
    }

    // Convert to double if it's an integer
    double value;
    if (value_node->data_type == SQL_TYPE_INT) {
        value = (double)value_node->value.int_value;
    } else if (value_node->data_type == SQL_TYPE_DOUBLE) {
        value = value_node->value.double_value;
    } else {
        sql_ctx_error(ctx, "ROUND requires the parameter to be DOUBLE or INT.");
        return sql_double_init(ctx, 0, true);
    }

    // Perform rounding to the nearest integer
    double rounded_value = round(value);

    return sql_double_init(ctx, rounded_value, false);
}

static sql_node_t *sql_func_round_with_decimal_places(sql_ctx_t *ctx, sql_node_t *f) {
    // Evaluate the first parameter
    sql_node_t *value_node = sql_eval(ctx, f->parameters[0]);
    if (!value_node || value_node->is_null) {
        return sql_double_init(ctx, 0, true);
    }

    // Convert to double if it's an integer
    double value;
    if (value_node->data_type == SQL_TYPE_INT) {
        value = (double)value_node->value.int_value;
    } else if (value_node->data_type == SQL_TYPE_DOUBLE) {
        value = value_node->value.double_value;
    } else {
        sql_ctx_error(ctx, "ROUND requires the first parameter to be DOUBLE or INT.");
        return sql_double_init(ctx, 0, true);
    }

    // Evaluate the second parameter
    sql_node_t *decimal_places_node = sql_eval(ctx, f->parameters[1]);
    if (!decimal_places_node || decimal_places_node->is_null || decimal_places_node->data_type != SQL_TYPE_INT) {
        sql_ctx_error(ctx, "ROUND's second parameter must be a valid INT.");
        return sql_double_init(ctx, 0, true);
    }
    int decimal_places = decimal_places_node->value.int_value;

    // Perform rounding to the specified decimal places
    double factor = pow(10.0, decimal_places);
    double rounded_value = round(value * factor) / factor;

    return sql_double_init(ctx, rounded_value, false);
}

static sql_node_t *sql_func_floor(sql_ctx_t *ctx, sql_node_t *f) {
    // Evaluate the parameter
    sql_node_t *value_node = sql_eval(ctx, f->parameters[0]);
    if (!value_node || value_node->is_null) {
        return sql_double_init(ctx, 0, true);
    }

    // Convert to double if it's an integer
    double value;
    if (value_node->data_type == SQL_TYPE_INT) {
        value = (double)value_node->value.int_value;
    } else if (value_node->data_type == SQL_TYPE_DOUBLE) {
        value = value_node->value.double_value;
    } else {
        sql_ctx_error(ctx, "FLOOR requires the parameter to be DOUBLE or INT.");
        return sql_double_init(ctx, 0, true);
    }

    // Perform floor operation
    double floored_value = floor(value);

    return sql_double_init(ctx, floored_value, false);
}

static sql_node_t *sql_func_ceil(sql_ctx_t *ctx, sql_node_t *f) {
    // Evaluate the parameter
    sql_node_t *value_node = sql_eval(ctx, f->parameters[0]);
    if (!value_node || value_node->is_null) {
        return sql_double_init(ctx, 0, true);
    }

    // Convert to double if it's an integer
    double value;
    if (value_node->data_type == SQL_TYPE_INT) {
        value = (double)value_node->value.int_value;
    } else if (value_node->data_type == SQL_TYPE_DOUBLE) {
        value = value_node->value.double_value;
    } else {
        sql_ctx_error(ctx, "CEIL requires the parameter to be DOUBLE or INT.");
        return sql_double_init(ctx, 0, true);
    }

    // Perform ceiling operation
    double ceiled_value = ceil(value);

    return sql_double_init(ctx, ceiled_value, false);
}

static sql_ctx_spec_update_t *update_round_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters < 1 || f->num_parameters > 2) {
        sql_ctx_error(ctx, "ROUND requires one or two parameters.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = f->num_parameters;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, f->num_parameters * sizeof(sql_data_type_t));

    // First parameter must be DOUBLE or INT
    update->expected_data_types[0] = SQL_TYPE_DOUBLE; // Allow INT but treat it as DOUBLE

    // Optional second parameter must be INT
    if (f->num_parameters == 2) {
        update->expected_data_types[1] = SQL_TYPE_INT;
    }

    // Determine implementation based on parameter count
    if (f->num_parameters == 1) {
        update->implementation = sql_func_round;
    } else {
        update->implementation = sql_func_round_with_decimal_places;
    }

    update->return_type = SQL_TYPE_DOUBLE;
    return update;
}

static sql_ctx_spec_t round_spec = {
    .name = "ROUND",
    .description = "Rounds a number to the nearest integer or specified decimal places.",
    .update = update_round_spec
};

static sql_ctx_spec_update_t *update_floor_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 1) {
        sql_ctx_error(ctx, "FLOOR requires exactly one parameter.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 1;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, sizeof(sql_data_type_t));
    update->expected_data_types[0] = SQL_TYPE_DOUBLE; // Allow INT but treat it as DOUBLE

    update->return_type = SQL_TYPE_DOUBLE;
    update->implementation = sql_func_floor;
    return update;
}

static sql_ctx_spec_t floor_spec = {
    .name = "FLOOR",
    .description = "Rounds a number down to the nearest integer.",
    .update = update_floor_spec
};

static sql_ctx_spec_update_t *update_ceil_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 1) {
        sql_ctx_error(ctx, "CEIL requires exactly one parameter.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 1;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, sizeof(sql_data_type_t));
    update->expected_data_types[0] = SQL_TYPE_DOUBLE; // Allow INT but treat it as DOUBLE

    update->return_type = SQL_TYPE_DOUBLE;
    update->implementation = sql_func_ceil;
    return update;
}

static sql_ctx_spec_t ceil_spec = {
    .name = "CEIL",
    .description = "Rounds a number up to the nearest integer.",
    .update = update_ceil_spec
};

void sql_register_round(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &round_spec);
    sql_ctx_register_spec(ctx, &floor_spec);
    sql_ctx_register_spec(ctx, &ceil_spec);

    sql_ctx_register_callback(ctx, sql_func_round, "round", "Rounds a number to the nearest integer.");
    sql_ctx_register_callback(ctx, sql_func_round_with_decimal_places, "round_with_decimal_places", "Rounds a number to the specified number of decimal places.");
    sql_ctx_register_callback(ctx, sql_func_floor, "floor", "Rounds a number down to the nearest integer.");
    sql_ctx_register_callback(ctx, sql_func_ceil, "ceil", "Rounds a number up to the nearest integer.");
}
