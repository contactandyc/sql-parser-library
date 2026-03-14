// SPDX-FileCopyrightText: 2024-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai — technical questions: contact Andy (above)
// SPDX-License-Identifier: Apache-2.0

#include "sql-parser-library/sql_node.h"
#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_interval.h"

// Helper to calculate the modified time
static time_t adjust_time_by_seconds(time_t base_time, double seconds) {
    return base_time + (time_t)seconds;
}

sql_node_t *sql_int_add(sql_ctx_t *ctx, sql_node_t *f) {
    int result = 0;
    for( size_t i = 0; i < f->num_parameters; i++ ) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (!child || child->is_null) {
            return sql_int_init(ctx, 0, true);
        }
        result += child->value.int_value;
    }
    return sql_int_init(ctx, result, false);
}

sql_node_t *sql_int_subtract(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_int_init(ctx, 0, true);
    }
    return sql_int_init(ctx, left->value.int_value - right->value.int_value, false);
}

sql_node_t *sql_int_multiply(sql_ctx_t *ctx, sql_node_t *f) {
    int result = 1;
    for( size_t i = 0; i < f->num_parameters; i++ ) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (!child || child->is_null) {
            return sql_int_init(ctx, 0, true);
        }
        result *= child->value.int_value;
    }
    return sql_int_init(ctx, result, false);
}

sql_node_t *sql_int_divide(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null || right->value.int_value == 0) {
        return sql_int_init(ctx, 0, true);
    }
    double a = left->value.int_value;
    double b = right->value.int_value;
    return sql_double_init(ctx, a / b, false);
}

sql_node_t *sql_double_add(sql_ctx_t *ctx, sql_node_t *f) {
    double result = 0;
    for( size_t i = 0; i < f->num_parameters; i++ ) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (!child || child->is_null) {
            return sql_double_init(ctx, 0, true);
        }
        result += child->value.double_value;
    }
    return sql_double_init(ctx, result, false);
}

sql_node_t *sql_double_subtract(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_double_init(ctx, 0, true);
    }
    return sql_double_init(ctx, left->value.double_value - right->value.double_value, false);
}

sql_node_t *sql_double_multiply(sql_ctx_t *ctx, sql_node_t *f) {
    double result = 1;
    for( size_t i = 0; i < f->num_parameters; i++ ) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (!child || child->is_null) {
            return sql_double_init(ctx, 0, true);
        }
        result *= child->value.double_value;
    }
    return sql_double_init(ctx, result, false);
}

sql_node_t *sql_double_divide(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null || right->value.double_value == 0) {
        return sql_double_init(ctx, 0, true);
    }
    return sql_double_init(ctx, left->value.double_value / right->value.double_value, false);
}

sql_node_t *sql_string_add(sql_ctx_t *ctx, sql_node_t *f) {
    char *result = NULL;
    for( size_t i = 0; i < f->num_parameters; i++ ) {
        sql_node_t *child = sql_eval(ctx, f->parameters[i]);
        if (!child || child->is_null) {
            return sql_string_init(ctx, NULL, true);
        }
        if (!result) {
            result = aml_pool_strdup(ctx->pool, child->value.string_value);
        }
        else {
            result = aml_pool_strdupf(ctx->pool, "%s%s", result, child->value.string_value);
        }
    }
    return sql_string_init(ctx, result, false);
}

// Add an integer value to a datetime (assumes days)
sql_node_t *sql_datetime_int_add(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *datetime_node = sql_eval(ctx, f->parameters[0]);
    sql_node_t *int_node = sql_eval(ctx, f->parameters[1]);

    if (!datetime_node || datetime_node->is_null ||
        !int_node || int_node->is_null) {
        return sql_datetime_init(ctx, 0, true); // Return NULL datetime
    }

    time_t adjusted_time = adjust_time_by_seconds(datetime_node->value.epoch, int_node->value.int_value * 86400);
    return sql_datetime_init(ctx, adjusted_time, false);
}

// Subtract an integer value from a datetime (assumes days)
sql_node_t *sql_datetime_int_subtract(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *datetime_node = sql_eval(ctx, f->parameters[0]);
    sql_node_t *int_node = sql_eval(ctx, f->parameters[1]);

    if (!datetime_node || datetime_node->is_null ||
        !int_node || int_node->is_null) {
        return sql_datetime_init(ctx, 0, true); // Return NULL datetime
    }

    time_t adjusted_time = adjust_time_by_seconds(datetime_node->value.epoch, -(int_node->value.int_value * 86400));
    return sql_datetime_init(ctx, adjusted_time, false);
}

// Add a double value to a datetime (assumes fractional days)
sql_node_t *sql_datetime_double_add(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *datetime_node = sql_eval(ctx, f->parameters[0]);
    sql_node_t *double_node = sql_eval(ctx, f->parameters[1]);

    if (!datetime_node || datetime_node->is_null ||
        !double_node || double_node->is_null) {
        return sql_datetime_init(ctx, 0, true); // Return NULL datetime
    }

    time_t adjusted_time = adjust_time_by_seconds(datetime_node->value.epoch, double_node->value.double_value * 86400);
    return sql_datetime_init(ctx, adjusted_time, false);
}

// Subtract a double value from a datetime (assumes fractional days)
sql_node_t *sql_datetime_double_subtract(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *datetime_node = sql_eval(ctx, f->parameters[0]);
    sql_node_t *double_node = sql_eval(ctx, f->parameters[1]);

    if (!datetime_node || datetime_node->is_null ||
        !double_node || double_node->is_null) {
        return sql_datetime_init(ctx, 0, true); // Return NULL datetime
    }

    time_t adjusted_time = adjust_time_by_seconds(datetime_node->value.epoch, -(double_node->value.double_value * 86400));
    return sql_datetime_init(ctx, adjusted_time, false);
}

// Subtract two datetime values (returns seconds between them)
sql_node_t *sql_datetime_subtract(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *left_node = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right_node = sql_eval(ctx, f->parameters[1]);

    if (!left_node || left_node->is_null ||
        !right_node || right_node->is_null) {
        return sql_double_init(ctx, 0, true); // Return NULL result
    }

    double seconds_diff = difftime(left_node->value.epoch, right_node->value.epoch);
    return sql_double_init(ctx, seconds_diff, false);
}

// Add an interval string to a datetime
sql_node_t *sql_datetime_interval_add(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *datetime_node = sql_eval(ctx, f->parameters[0]);
    sql_node_t *interval_node = sql_eval(ctx, f->parameters[1]);

    if (!datetime_node || datetime_node->is_null ||
        !interval_node || interval_node->is_null) {
        return sql_datetime_init(ctx, 0, true); // Return NULL datetime
    }

    sql_interval_t *interval = sql_interval_parse(ctx, interval_node->value.string_value);
    if (!interval) {
        return sql_datetime_init(ctx, 0, true); // Return NULL datetime
    }

    struct tm tm_info = {0};
    gmtime_r(&datetime_node->value.epoch, &tm_info);

    tm_info.tm_year += interval->years;
    tm_info.tm_mon += interval->months;
    tm_info.tm_mday += interval->days;
    tm_info.tm_hour += interval->hours;
    tm_info.tm_min += interval->minutes;
    tm_info.tm_sec += interval->seconds;

    time_t adjusted_time = timegm(&tm_info) + (interval->microseconds / 1000000);
    return sql_datetime_init(ctx, adjusted_time, false);
}

// Subtract an interval string from a datetime
sql_node_t *sql_datetime_interval_subtract(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *datetime_node = sql_eval(ctx, f->parameters[0]);
    sql_node_t *interval_node = sql_eval(ctx, f->parameters[1]);

    if (!datetime_node || datetime_node->is_null ||
        !interval_node || interval_node->is_null) {
        return sql_datetime_init(ctx, 0, true); // Return NULL datetime
    }

    sql_interval_t *interval = sql_interval_parse(ctx, interval_node->value.string_value);
    if (!interval) {
        return sql_datetime_init(ctx, 0, true); // Return NULL datetime
    }

    struct tm tm_info;
    gmtime_r(&datetime_node->value.epoch, &tm_info);

    sql_ctx_warning( ctx, "Interval %d %d %d %d %d %d %d\n", interval->years, interval->months, interval->days, interval->hours, interval->minutes, interval->seconds, interval->microseconds);
    sql_ctx_warning( ctx, "Time %d %d %d %d %d %d\n", tm_info.tm_year, tm_info.tm_mon, tm_info.tm_mday, tm_info.tm_hour, tm_info.tm_min, tm_info.tm_sec);

    tm_info.tm_year -= interval->years;
    tm_info.tm_mon -= interval->months;
    tm_info.tm_mday -= interval->days;
    tm_info.tm_hour -= interval->hours;
    tm_info.tm_min -= interval->minutes;
    tm_info.tm_sec -= interval->seconds;

    time_t adjusted_time = timegm(&tm_info) - (interval->microseconds / 1000000);
    return sql_datetime_init(ctx, adjusted_time, false);
}

static sql_ctx_spec_update_t *update_arithmetic_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters < 2) {
        sql_ctx_error(ctx, "Arithmetic operations require at least two parameters.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = f->num_parameters;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, f->num_parameters * sizeof(sql_data_type_t));

    sql_data_type_t data_type = f->parameters[0]->data_type;
    for (size_t i = 0; i < f->num_parameters; i++) {
        if (f->parameters[i]->data_type != data_type) {
            if ((data_type == SQL_TYPE_INT && f->parameters[i]->data_type == SQL_TYPE_DOUBLE) ||
                (data_type == SQL_TYPE_DOUBLE && f->parameters[i]->data_type == SQL_TYPE_INT)) {
                data_type = SQL_TYPE_DOUBLE; // Promote to double if there's a mix of int and double
            }
        }
        update->expected_data_types[i] = data_type;
    }
    if(data_type == SQL_TYPE_DATETIME && f->parameters[1]->data_type == SQL_TYPE_STRING && f->parameters[1]->token_type == SQL_COMPOUND_LITERAL) {
        update->expected_data_types[1] = SQL_TYPE_STRING; // Special case: interval string
    }

    update->return_type = data_type;
    const char *name = spec->name;

    // Determine the correct implementation based on the operator and type
    if (data_type == SQL_TYPE_INT) {
        if (strcmp(name, "+") == 0) update->implementation = sql_int_add;
        else if (strcmp(name, "-") == 0) update->implementation = sql_int_subtract;
        else if (strcmp(name, "*") == 0) update->implementation = sql_int_multiply;
        else if (strcmp(name, "/") == 0) {
            update->implementation = sql_int_divide;
            update->return_type = SQL_TYPE_DOUBLE; // Division of two integers gives a double
        }
    } else if (data_type == SQL_TYPE_DOUBLE) {
        if (strcmp(name, "+") == 0) update->implementation = sql_double_add;
        else if (strcmp(name, "-") == 0) update->implementation = sql_double_subtract;
        else if (strcmp(name, "*") == 0) update->implementation = sql_double_multiply;
        else if (strcmp(name, "/") == 0) update->implementation = sql_double_divide;
    } else if (data_type == SQL_TYPE_STRING) {
        if (strcmp(name, "+") == 0) update->implementation = sql_string_add;
    } else if (data_type == SQL_TYPE_DATETIME) {
        if (strcmp(name, "+") == 0 && f->parameters[1]->data_type == SQL_TYPE_INT)
            update->implementation = sql_datetime_int_add;
        else if (strcmp(name, "-") == 0 && f->parameters[1]->data_type == SQL_TYPE_INT)
            update->implementation = sql_datetime_int_subtract;
        else if (strcmp(name, "+") == 0 && f->parameters[1]->data_type == SQL_TYPE_DOUBLE)
            update->implementation = sql_datetime_double_add;
        else if (strcmp(name, "-") == 0 && f->parameters[1]->data_type == SQL_TYPE_DOUBLE)
            update->implementation = sql_datetime_double_subtract;
        else if (strcmp(name, "-") == 0 && f->parameters[1]->data_type == SQL_TYPE_DATETIME) {
            update->implementation = sql_datetime_subtract;
            update->return_type = SQL_TYPE_DOUBLE; // Special case: subtracting two datetimes gives a double
        } else if (strcmp(name, "+") == 0 && f->parameters[1]->data_type == SQL_TYPE_STRING)
            update->implementation = sql_datetime_interval_add;
        else if (strcmp(name, "-") == 0 && f->parameters[1]->data_type == SQL_TYPE_STRING)
            update->implementation = sql_datetime_interval_subtract;
        else {
            sql_ctx_error(ctx, "Unsupported datetime arithmetic operation.");
            return NULL;
        }
    } else {
        sql_ctx_error(ctx, "Arithmetic operation not supported for data type %s.", sql_data_type_name(data_type));
        return NULL;
    }

    return update;
}

sql_ctx_spec_t add_spec = {
    .name = "+",
    .description = "Addition operator",
    .update = update_arithmetic_spec
};

sql_ctx_spec_t subtract_spec = {
    .name = "-",
    .description = "Subtraction operator",
    .update = update_arithmetic_spec
};

sql_ctx_spec_t multiply_spec = {
    .name = "*",
    .description = "Multiplication operator",
    .update = update_arithmetic_spec
};

sql_ctx_spec_t divide_spec = {
    .name = "/",
    .description = "Division operator",
    .update = update_arithmetic_spec
};

void sql_register_arithmetic(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &add_spec);
    sql_ctx_register_spec(ctx, &subtract_spec);
    sql_ctx_register_spec(ctx, &multiply_spec);
    sql_ctx_register_spec(ctx, &divide_spec);

    sql_ctx_register_callback(ctx, sql_int_add, "int_add", "INT + INT");
    sql_ctx_register_callback(ctx, sql_int_subtract, "int_subtract", "INT - INT");
    sql_ctx_register_callback(ctx, sql_int_multiply, "int_multiply", "INT * INT");
    sql_ctx_register_callback(ctx, sql_int_divide, "int_divide", "INT / INT - returns DOUBLE");

    sql_ctx_register_callback(ctx, sql_double_add, "double_add", "DOUBLE + DOUBLE");
    sql_ctx_register_callback(ctx, sql_double_subtract, "double_subtract", "DOUBLE - DOUBLE");
    sql_ctx_register_callback(ctx, sql_double_multiply, "double_multiply", "DOUBLE * DOUBLE");
    sql_ctx_register_callback(ctx, sql_double_divide, "double_divide", "DOUBLE / DOUBLE");

    sql_ctx_register_callback(ctx, sql_string_add, "string_add", "STRING + STRING");

    sql_ctx_register_callback(ctx, sql_datetime_int_add, "datetime_int_add", "Adds days to a DATETIME");
    sql_ctx_register_callback(ctx, sql_datetime_int_subtract, "datetime_int_subtract", "Subtracts days from a DATETIME");
    sql_ctx_register_callback(ctx, sql_datetime_double_add, "datetime_double_add", "Adds fractional days to a DATETIME");
    sql_ctx_register_callback(ctx, sql_datetime_double_subtract, "datetime_double_subtract", "Subtracts fractional days from a DATETIME");
    sql_ctx_register_callback(ctx, sql_datetime_subtract, "datetime_subtract", "Subtracts two DATETIME values (returns seconds)");
    sql_ctx_register_callback(ctx, sql_datetime_interval_add, "datetime_interval_add", "Adds an INTERVAL to a DATETIME");
    sql_ctx_register_callback(ctx, sql_datetime_interval_subtract, "datetime_interval_subtract", "Subtracts an INTERVAL from a DATETIME");
}
