// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/date_utils.h"
#include <time.h>
#include <strings.h>

// Function to truncate to the start of the second
static sql_node_t *sql_trunc_second(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_datetime_init(ctx, 0, true);
    }

    // No truncation needed for seconds, as time_t already uses second precision
    return sql_datetime_init(ctx, child->value.epoch, false);
}

// Function to truncate to the start of the minute
static sql_node_t *sql_trunc_minute(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_datetime_init(ctx, 0, true);
    }

    struct tm *dt = gmtime(&child->value.epoch);
    dt->tm_sec = 0; // Reset seconds

    time_t truncated = timegm(dt);
    return sql_datetime_init(ctx, truncated, false);
}

// Function to truncate to the start of the hour
static sql_node_t *sql_trunc_hour(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_datetime_init(ctx, 0, true);
    }

    struct tm *dt = gmtime(&child->value.epoch);
    dt->tm_min = 0; // Reset minutes
    dt->tm_sec = 0; // Reset seconds

    time_t truncated = timegm(dt);
    return sql_datetime_init(ctx, truncated, false);
}

// Function to truncate to the start of the day
static sql_node_t *sql_trunc_day(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_datetime_init(ctx, 0, true);
    }

    struct tm *dt = gmtime(&child->value.epoch);
    dt->tm_hour = 0; // Reset hours
    dt->tm_min = 0;  // Reset minutes
    dt->tm_sec = 0;  // Reset seconds

    time_t truncated = timegm(dt);
    return sql_datetime_init(ctx, truncated, false);
}

// Function to truncate to the start of the week
static sql_node_t *sql_trunc_week(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_datetime_init(ctx, 0, true);
    }

    struct tm *dt = gmtime(&child->value.epoch);
    dt->tm_hour = 0;
    dt->tm_min = 0;
    dt->tm_sec = 0;
    dt->tm_mday -= dt->tm_wday; // Move to the start of the week (Sunday)

    time_t truncated = timegm(dt);
    return sql_datetime_init(ctx, truncated, false);
}

// Function to truncate to the start of the month
static sql_node_t *sql_trunc_month(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_datetime_init(ctx, 0, true);
    }

    struct tm *dt = gmtime(&child->value.epoch);
    dt->tm_mday = 1; // Reset to the first day of the month
    dt->tm_hour = 0;
    dt->tm_min = 0;
    dt->tm_sec = 0;

    time_t truncated = timegm(dt);
    return sql_datetime_init(ctx, truncated, false);
}

// Function to truncate to the start of the quarter
static sql_node_t *sql_trunc_quarter(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_datetime_init(ctx, 0, true);
    }

    struct tm *dt = gmtime(&child->value.epoch);
    dt->tm_mon = (dt->tm_mon / 3) * 3; // Move to the first month of the quarter
    dt->tm_mday = 1;
    dt->tm_hour = 0;
    dt->tm_min = 0;
    dt->tm_sec = 0;

    time_t truncated = timegm(dt);
    return sql_datetime_init(ctx, truncated, false);
}

// Function to truncate to the start of the year
static sql_node_t *sql_trunc_year(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_datetime_init(ctx, 0, true);
    }

    struct tm *dt = gmtime(&child->value.epoch);
    dt->tm_mon = 0;   // Reset to January
    dt->tm_mday = 1;  // Reset to the first day of the year
    dt->tm_hour = 0;
    dt->tm_min = 0;
    dt->tm_sec = 0;

    time_t truncated = timegm(dt);
    return sql_datetime_init(ctx, truncated, false);
}

// Function to truncate to the start of the decade
static sql_node_t *sql_trunc_decade(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_datetime_init(ctx, 0, true);
    }

    struct tm *dt = gmtime(&child->value.epoch);
    dt->tm_year = (dt->tm_year / 10) * 10; // Reset to the start of the decade
    dt->tm_mon = 0;
    dt->tm_mday = 1;
    dt->tm_hour = 0;
    dt->tm_min = 0;
    dt->tm_sec = 0;

    time_t truncated = timegm(dt);
    return sql_datetime_init(ctx, truncated, false);
}

// Function to truncate to the start of the century
static sql_node_t *sql_trunc_century(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_datetime_init(ctx, 0, true);
    }

    struct tm *dt = gmtime(&child->value.epoch);
    dt->tm_year = (dt->tm_year / 100) * 100; // Reset to the start of the century
    dt->tm_mon = 0;
    dt->tm_mday = 1;
    dt->tm_hour = 0;
    dt->tm_min = 0;
    dt->tm_sec = 0;

    time_t truncated = timegm(dt);
    return sql_datetime_init(ctx, truncated, false);
}

// Function to truncate to the start of the millennium
static sql_node_t *sql_trunc_millennium(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_datetime_init(ctx, 0, true);
    }

    struct tm *dt = gmtime(&child->value.epoch);
    dt->tm_year = (dt->tm_year / 1000) * 1000; // Reset to the start of the millennium
    dt->tm_mon = 0;
    dt->tm_mday = 1;
    dt->tm_hour = 0;
    dt->tm_min = 0;
    dt->tm_sec = 0;

    time_t truncated = timegm(dt);
    return sql_datetime_init(ctx, truncated, false);
}

// Function to get the appropriate truncation function
sql_node_cb get_trunc_function(const char *part) {
    if (strcasecmp(part, "SECOND") == 0)
        return sql_trunc_second;
    else if (strcasecmp(part, "MINUTE") == 0)
        return sql_trunc_minute;
    else if (strcasecmp(part, "HOUR") == 0)
        return sql_trunc_hour;
    else if (strcasecmp(part, "DAY") == 0)
        return sql_trunc_day;
    else if (strcasecmp(part, "WEEK") == 0)
        return sql_trunc_week;
    else if (strcasecmp(part, "MONTH") == 0)
        return sql_trunc_month;
    else if (strcasecmp(part, "QUARTER") == 0)
        return sql_trunc_quarter;
    else if (strcasecmp(part, "YEAR") == 0)
        return sql_trunc_year;
    else if (strcasecmp(part, "DECADE") == 0)
        return sql_trunc_decade;
    else if (strcasecmp(part, "CENTURY") == 0)
        return sql_trunc_century;
    else if (strcasecmp(part, "MILLENNIUM") == 0)
        return sql_trunc_millennium;
    return NULL;
}

// Function to check if the truncation part is valid
bool is_valid_trunc(const char *value) {
    return get_trunc_function(value) != NULL;
}

static sql_ctx_spec_update_t *update_trunc_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 2) {
        sql_ctx_error(ctx, "DATE_TRUNC function requires exactly two parameters: part and datetime.");
        return NULL;
    }

    sql_node_t *part_node = f->parameters[0];
    sql_node_t *datetime_node = f->parameters[1];

    if (part_node->data_type != SQL_TYPE_STRING || datetime_node->data_type != SQL_TYPE_DATETIME) {
        sql_ctx_error(ctx, "Invalid parameter types for DATE_TRUNC. Expected (STRING, DATETIME).");
        return NULL;
    }

    sql_node_cb trunc_func = get_trunc_function(part_node->value.string_value);
    if (!trunc_func) {
        sql_ctx_error(ctx, "Invalid part specified for DATE_TRUNC: %s", part_node->value.string_value);
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 1;
    update->parameters = f->parameters + 1; // Only pass the datetime node to the implementation
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, sizeof(sql_data_type_t));
    update->expected_data_types[0] = SQL_TYPE_DATETIME;
    update->return_type = SQL_TYPE_DATETIME;
    update->implementation = trunc_func;

    return update;
}

// Specification for DATE_TRUNC function
sql_ctx_spec_t date_trunc_spec = {
    .name = "DATE_TRUNC",
    .description = "Truncates a DATETIME value to a specified part.",
    .update = update_trunc_spec
};

// Function to register DATE_TRUNC
void sql_register_date_trunc(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &date_trunc_spec);

    // Register individual truncation functions
    sql_ctx_register_callback(ctx, sql_trunc_second, "trunc_second", "Truncates a DATETIME to the second.");
    sql_ctx_register_callback(ctx, sql_trunc_minute, "trunc_minute", "Truncates a DATETIME to the minute.");
    sql_ctx_register_callback(ctx, sql_trunc_hour, "trunc_hour", "Truncates a DATETIME to the hour.");
    sql_ctx_register_callback(ctx, sql_trunc_day, "trunc_day", "Truncates a DATETIME to the day.");
    sql_ctx_register_callback(ctx, sql_trunc_week, "trunc_week", "Truncates a DATETIME to the week.");
    sql_ctx_register_callback(ctx, sql_trunc_month, "trunc_month", "Truncates a DATETIME to the month.");
    sql_ctx_register_callback(ctx, sql_trunc_quarter, "trunc_quarter", "Truncates a DATETIME to the quarter.");
    sql_ctx_register_callback(ctx, sql_trunc_year, "trunc_year", "Truncates a DATETIME to the year.");
    sql_ctx_register_callback(ctx, sql_trunc_decade, "trunc_decade", "Truncates a DATETIME to the decade.");
    sql_ctx_register_callback(ctx, sql_trunc_century, "trunc_century", "Truncates a DATETIME to the century.");
    sql_ctx_register_callback(ctx, sql_trunc_millennium, "trunc_millennium", "Truncates a DATETIME to the millennium.");
}
