// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"
#include <limits.h>
#include <float.h>
#include <strings.h>

static sql_node_t *sql_bool_min(sql_ctx_t *ctx, sql_node_t *f) {
    // set bool to max value
    bool result = true;
    for( size_t i = 0; i < f->num_parameters; i++ ) {
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

static sql_node_t *sql_bool_max(sql_ctx_t *ctx, sql_node_t *f) {
    // set bool to min value
    bool result = false;
    for( size_t i = 0; i < f->num_parameters; i++ ) {
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

static sql_node_t *sql_string_min(sql_ctx_t *ctx, sql_node_t *f) {
    // set string to max value
    const char *result = NULL;
    for( size_t i = 0; i < f->num_parameters; i++ ) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (!child || child->is_null) {
            return sql_string_init(ctx, NULL, true);
        }
        if (!result || strcasecmp(child->value.string_value, result) < 0) {
            result = child->value.string_value;
        }
    }
    return sql_string_init(ctx, result, false);
}

static sql_node_t *sql_string_max(sql_ctx_t *ctx, sql_node_t *f) {
    // set string to min value
    const char *result = NULL;
    for( size_t i = 0; i < f->num_parameters; i++ ) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (!child || child->is_null) {
            return sql_string_init(ctx, NULL, true);
        }
        if (!result || strcasecmp(child->value.string_value, result) > 0) {
            result = child->value.string_value;
        }
    }
    return sql_string_init(ctx, result, false);
}

static sql_node_t *sql_datetime_min(sql_ctx_t *ctx, sql_node_t *f) {
    // set date to max value
    time_t result = LONG_MAX;
    for( size_t i = 0; i < f->num_parameters; i++ ) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (!child || child->is_null) {
            return sql_datetime_init(ctx, 0, true);
        }
        if (child->value.epoch < result) {
            result = child->value.epoch;
        }
    }
    return sql_datetime_init(ctx, result, false);
}

static sql_node_t *sql_datetime_max(sql_ctx_t *ctx, sql_node_t *f) {
    // set date to min value
    time_t result = LONG_MIN;
    for( size_t i = 0; i < f->num_parameters; i++ ) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (!child || child->is_null) {
            return sql_datetime_init(ctx, 0, true);
        }
        if (child->value.epoch > result) {
            result = child->value.epoch;
        }
    }
    return sql_datetime_init(ctx, result, false);
}

static sql_node_t *sql_int_min(sql_ctx_t *ctx, sql_node_t *f) {
    // set int to max value
    int result = INT_MAX;
    for( size_t i = 0; i < f->num_parameters; i++ ) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (!child || child->is_null) {
            return sql_int_init(ctx, 0, true);
        }
        if (child->value.int_value < result) {
            result = child->value.int_value;
        }
    }
    return sql_int_init(ctx, result, false);
}

static sql_node_t *sql_int_max(sql_ctx_t *ctx, sql_node_t *f) {
    // set int to min value
    int result = INT_MIN;
    for( size_t i = 0; i < f->num_parameters; i++ ) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (!child || child->is_null) {
            return sql_int_init(ctx, 0, true);
        }
        if (child->value.int_value > result) {
            result = child->value.int_value;
        }
    }
    return sql_int_init(ctx, result, false);
}

static sql_node_t *sql_double_min(sql_ctx_t *ctx, sql_node_t *f) {
    // set double to max value
    double result = DBL_MAX;
    for( size_t i = 0; i < f->num_parameters; i++ ) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (!child || child->is_null) {
            return sql_double_init(ctx, 0, true);
        }
        if (child->value.double_value < result) {
            result = child->value.double_value;
        }
    }
    return sql_double_init(ctx, result, false);
}

static sql_node_t *sql_double_max(sql_ctx_t *ctx, sql_node_t *f) {
    // set double to min value
    double result = DBL_MIN;
    for( size_t i = 0; i < f->num_parameters; i++ ) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (!child || child->is_null) {
            return sql_double_init(ctx, 0, true);
        }
        if (child->value.double_value > result) {
            result = child->value.double_value;
        }
    }
    return sql_double_init(ctx, result, false);
}

static sql_ctx_spec_update_t *update_min_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters < 1) {
        sql_ctx_error(ctx, "MIN function requires at least one parameter.");
        return NULL;
    }

    // Determine the common type
    sql_data_type_t common_type = f->parameters[0]->data_type;
    if(common_type == SQL_TYPE_INT) {
        for (size_t i = 1; i < f->num_parameters; i++) {
            sql_data_type_t param_type = f->parameters[i]->data_type;
            if (param_type == SQL_TYPE_DOUBLE) {
                common_type = SQL_TYPE_DOUBLE;  // Promote to double
            }
        }
    }

    // Allocate and initialize the update structure
    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));

    // Set the number of parameters
    update->num_parameters = f->num_parameters;

    // Allocate memory for expected parameter types
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, f->num_parameters * sizeof(sql_data_type_t));
    update->parameters = f->parameters;

    // Set all parameters to the determined common type
    for (size_t i = 0; i < f->num_parameters; i++) {
        update->expected_data_types[i] = common_type;
    }

    // Assign the correct implementation and return type
    switch (common_type) {
        case SQL_TYPE_BOOL:
            update->implementation = sql_bool_min;
            update->return_type = SQL_TYPE_BOOL;
            break;
        case SQL_TYPE_STRING:
            update->implementation = sql_string_min;
            update->return_type = SQL_TYPE_STRING;
            break;
        case SQL_TYPE_DATETIME:
            update->implementation = sql_datetime_min;
            update->return_type = SQL_TYPE_DATETIME;
            break;
        case SQL_TYPE_INT:
            update->implementation = sql_int_min;
            update->return_type = SQL_TYPE_INT;
            break;
        case SQL_TYPE_DOUBLE:
            update->implementation = sql_double_min;
            update->return_type = SQL_TYPE_DOUBLE;
            break;
        default:
            sql_ctx_error(ctx, "Unsupported parameter type for MIN function.");
            return NULL;
    }

    return update;
}

sql_ctx_spec_t min_function_spec = {
    .name = "MIN",
    .description = "Returns the minimum value.",
    .update = update_min_spec
};

static sql_ctx_spec_update_t *update_max_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters < 1) {
        sql_ctx_error(ctx, "MAX function requires at least one parameter.");
        return NULL;
    }

    // Determine the common type
    sql_data_type_t common_type = f->parameters[0]->data_type;
    if(common_type == SQL_TYPE_INT) {
        for (size_t i = 1; i < f->num_parameters; i++) {
            sql_data_type_t param_type = f->parameters[i]->data_type;
            if (param_type == SQL_TYPE_DOUBLE) {
                common_type = SQL_TYPE_DOUBLE;  // Promote to double
            }
        }
    }

    // Allocate and initialize the update structure
    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));

    // Set the number of parameters
    update->num_parameters = f->num_parameters;

    // Allocate memory for expected parameter types
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, f->num_parameters * sizeof(sql_data_type_t));
    update->parameters = f->parameters;

    // Set all parameters to the determined common type
    for (size_t i = 0; i < f->num_parameters; i++) {
        update->expected_data_types[i] = common_type;
    }

    // Assign the correct implementation and return type
    switch (common_type) {
        case SQL_TYPE_BOOL:
            update->implementation = sql_bool_max;
            update->return_type = SQL_TYPE_BOOL;
            break;
        case SQL_TYPE_STRING:
            update->implementation = sql_string_max;
            update->return_type = SQL_TYPE_STRING;
            break;
        case SQL_TYPE_DATETIME:
            update->implementation = sql_datetime_max;
            update->return_type = SQL_TYPE_DATETIME;
            break;
        case SQL_TYPE_INT:
            update->implementation = sql_int_max;
            update->return_type = SQL_TYPE_INT;
            break;
        case SQL_TYPE_DOUBLE:
            update->implementation = sql_double_max;
            update->return_type = SQL_TYPE_DOUBLE;
            break;
        default:
            sql_ctx_error(ctx, "Unsupported parameter type for MAX function.");
            return NULL;
    }

    return update;
}

sql_ctx_spec_t max_function_spec = {
    .name = "MAX",
    .description = "Returns the maximum value.",
    .update = update_max_spec
};

void sql_register_min_max(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &min_function_spec);
    sql_ctx_register_spec(ctx, &max_function_spec);

    sql_ctx_register_callback(ctx, sql_bool_min, "bool_min",
                              "Returns the minimum value of a boolean list.");
    sql_ctx_register_callback(ctx, sql_bool_max, "bool_max",
                              "Returns the maximum value of a boolean list.");
    sql_ctx_register_callback(ctx, sql_string_min, "string_min",
                              "Returns the minimum value of a string list.");
    sql_ctx_register_callback(ctx, sql_string_max, "string_max",
                              "Returns the maximum value of a string list.");
    sql_ctx_register_callback(ctx, sql_datetime_min, "datetime_min",
                              "Returns the minimum value of a datetime list.");
    sql_ctx_register_callback(ctx, sql_datetime_max, "datetime_max",
                              "Returns the maximum value of a datetime list.");
    sql_ctx_register_callback(ctx, sql_int_min, "int_min",
                              "Returns the minimum value of an integer list.");
    sql_ctx_register_callback(ctx, sql_int_max, "int_max",
                              "Returns the maximum value of an integer list.");
    sql_ctx_register_callback(ctx, sql_double_min, "double_min",
                              "Returns the minimum value of a double list.");
    sql_ctx_register_callback(ctx, sql_double_max, "double_max",
                              "Returns the maximum value of a double list.");
}
