// SPDX-FileCopyrightText: 2024-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai — technical questions: contact Andy (above)
// SPDX-License-Identifier: Apache-2.0

#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/date_utils.h"
#include <time.h>
#include <strings.h>

// Function to extract the quarter
static sql_node_t *sql_extract_quarter(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_int_init(ctx, 0, true);
    }
    struct tm *dt = gmtime(&child->value.epoch);
    return sql_int_init(ctx, (dt->tm_mon / 3) + 1, false);
}

// Function to extract the ISO week number
static sql_node_t *sql_extract_week(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_int_init(ctx, 0, true);
    }
    struct tm *dt = gmtime(&child->value.epoch);
    char buffer[10];
    strftime(buffer, sizeof(buffer), "%V", dt); // ISO week number
    return sql_int_init(ctx, atoi(buffer), false);
}

// Function to extract the day of the year
static sql_node_t *sql_extract_doy(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_int_init(ctx, 0, true);
    }
    struct tm *dt = gmtime(&child->value.epoch);
    char buffer[10];
    strftime(buffer, sizeof(buffer), "%j", dt); // Day of the year
    return sql_int_init(ctx, atoi(buffer), false);
}

// Function to extract the day of the week (0 for Sunday)
static sql_node_t *sql_extract_dow(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_int_init(ctx, 0, true);
    }
    struct tm *dt = gmtime(&child->value.epoch);
    return sql_int_init(ctx, dt->tm_wday, false); // tm_wday: 0 for Sunday, 1 for Monday, etc.
}

// Function to extract the ISO day of the week (1 for Monday, 7 for Sunday)
static sql_node_t *sql_extract_isodow(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_int_init(ctx, 0, true);
    }
    struct tm *dt = gmtime(&child->value.epoch);
    int isodow = dt->tm_wday == 0 ? 7 : dt->tm_wday; // Convert 0 (Sunday) to 7
    return sql_int_init(ctx, isodow, false);
}

// Function to extract the year
static sql_node_t *sql_extract_year(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_int_init(ctx, 0, true); // Return NULL if invalid input
    }
    struct tm *dt = gmtime(&child->value.epoch);
    return sql_int_init(ctx, dt->tm_year + 1900, false); // tm_year is years since 1900
}

// Function to extract the month
static sql_node_t *sql_extract_month(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_int_init(ctx, 0, true); // Return NULL if invalid input
    }
    struct tm *dt = gmtime(&child->value.epoch);
    return sql_int_init(ctx, dt->tm_mon + 1, false); // tm_mon is months since January (0-11)
}

// Function to extract the day
static sql_node_t *sql_extract_day(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_int_init(ctx, 0, true); // Return NULL if invalid input
    }
    struct tm *dt = gmtime(&child->value.epoch);
    return sql_int_init(ctx, dt->tm_mday, false);
}

// Function to extract the hour
static sql_node_t *sql_extract_hour(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_int_init(ctx, 0, true); // Return NULL if invalid input
    }
    struct tm *dt = gmtime(&child->value.epoch);
    return sql_int_init(ctx, dt->tm_hour, false);
}

// Function to extract the minute
static sql_node_t *sql_extract_minute(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_int_init(ctx, 0, true); // Return NULL if invalid input
    }
    struct tm *dt = gmtime(&child->value.epoch);
    return sql_int_init(ctx, dt->tm_min, false);
}

// Function to extract the second
static sql_node_t *sql_extract_second(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null || child->data_type != SQL_TYPE_DATETIME) {
        return sql_int_init(ctx, 0, true); // Return NULL if invalid input
    }
    struct tm *dt = gmtime(&child->value.epoch);
    return sql_int_init(ctx, dt->tm_sec, false);
}

sql_node_cb get_extract_function(const char *field) {
    if(strcasecmp(field, "YEAR") == 0)
        return sql_extract_year;
    else if(strcasecmp(field, "MONTH") == 0)
        return sql_extract_month;
    else if(strcasecmp(field, "DAY") == 0)
        return sql_extract_day;
    else if(strcasecmp(field, "HOUR") == 0)
        return sql_extract_hour;
    else if(strcasecmp(field, "MINUTE") == 0)
        return sql_extract_minute;
    else if(strcasecmp(field, "SECOND") == 0)
        return sql_extract_second;
    else if(strcasecmp(field, "QUARTER") == 0)
        return sql_extract_quarter;
    else if(strcasecmp(field, "WEEK") == 0)
        return sql_extract_week;
    else if(strcasecmp(field, "DOY") == 0 || strcasecmp(field, "DAYOFYEAR") == 0)
        return sql_extract_doy;
    else if(strcasecmp(field, "DOW") == 0 || strcasecmp(field, "DAYOFWEEK") == 0)
        return sql_extract_dow;
    else if(strcasecmp(field, "ISODOW") == 0 || strcasecmp(field, "ISODAYOFWEEK") == 0)
        return sql_extract_isodow;
    return NULL;
}

bool is_valid_extract(const char *value) {
    if(get_extract_function(value) != NULL)
        return true;
    return false;
}

// Update function for EXTRACT
static sql_ctx_spec_update_t *update_extract_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 2) {
        sql_ctx_error(ctx, "EXTRACT function requires exactly two parameters: field datetime.");
        return NULL;
    }

    sql_node_t *field_node = f->parameters[0];
    sql_node_t *datetime_node = f->parameters[1];

    if (field_node->data_type != SQL_TYPE_STRING || datetime_node->data_type != SQL_TYPE_DATETIME) {
        sql_ctx_error(ctx, "Invalid parameter types for EXTRACT function. Expected (STRING, DATETIME).");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 1;
    update->parameters = f->parameters + 1; // Only the datetime node is passed to the implementation
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, sizeof(sql_data_type_t));
    update->expected_data_types[0] = SQL_TYPE_DATETIME;
    update->return_type = SQL_TYPE_INT;


    update->implementation = get_extract_function(field_node->value.string_value);
    if(update->implementation == NULL) {
        sql_ctx_error(ctx, "Invalid field specified for EXTRACT: %s", field_node->value.string_value);
        return NULL;
    }
    return update;
}

// Specification for EXTRACT function
sql_ctx_spec_t extract_spec = {
    .name = "EXTRACT",
    .description = "Extracts a specified date/time part from a DATETIME value.",
    .update = update_extract_spec
};

sql_ctx_spec_t datepart_spec = {
    .name = "DATEPART",
    .description = "Extracts a specified date/time part from a DATETIME value.",
    .update = update_extract_spec
};


static sql_ctx_spec_update_t *update_shorthand_extract_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 1) {
        sql_ctx_error(ctx, "%s function requires exactly one parameter: datetime.", spec->name);
        return NULL;
    }

    sql_node_t *datetime_node = f->parameters[0];
    if (datetime_node->data_type != SQL_TYPE_DATETIME) {
        sql_ctx_error(ctx, "Invalid parameter type for %s function. Expected DATETIME.", spec->name);
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 1;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, sizeof(sql_data_type_t));
    update->expected_data_types[0] = SQL_TYPE_DATETIME;
    update->return_type = SQL_TYPE_INT;

    update->implementation = get_extract_function(spec->name);
    if(update->implementation == NULL) {
        sql_ctx_error(ctx, "Invalid field specified for %s: %s", spec->name, spec->name);
        return NULL;
    }
    return update;
}

sql_ctx_spec_t year_spec = {
    .name = "YEAR",
    .description = "Returns the year from a DATETIME value.",
    .update = update_shorthand_extract_spec
};

sql_ctx_spec_t month_spec = {
    .name = "MONTH",
    .description = "Returns the month from a DATETIME value.",
    .update = update_shorthand_extract_spec
};

sql_ctx_spec_t day_spec = {
    .name = "DAY",
    .description = "Returns the day from a DATETIME value.",
    .update = update_shorthand_extract_spec
};

sql_ctx_spec_t hour_spec = {
    .name = "HOUR",
    .description = "Returns the hour from a DATETIME value.",
    .update = update_shorthand_extract_spec
};

sql_ctx_spec_t minute_spec = {
    .name = "MINUTE",
    .description = "Returns the minute from a DATETIME value.",
    .update = update_shorthand_extract_spec
};

sql_ctx_spec_t second_spec = {
    .name = "SECOND",
    .description = "Returns the second from a DATETIME value.",
    .update = update_shorthand_extract_spec
};

sql_ctx_spec_t quarter_spec = {
    .name = "QUARTER",
    .description = "Returns the quarter from a DATETIME value.",
    .update = update_shorthand_extract_spec
};

sql_ctx_spec_t week_spec = {
    .name = "WEEK",
    .description = "Returns the ISO week number from a DATETIME value.",
    .update = update_shorthand_extract_spec
};

sql_ctx_spec_t doy_spec = {
    .name = "DOY",
    .description = "Returns the day of the year from a DATETIME value.",
    .update = update_shorthand_extract_spec
};

sql_ctx_spec_t day_of_year_spec = {
    .name = "DAYOFYEAR",
    .description = "Returns the day of the year from a DATETIME value.",
    .update = update_shorthand_extract_spec
};

sql_ctx_spec_t dow_spec = {
    .name = "DOW",
    .description = "Returns the day of the week (0 for Sunday) from a DATETIME value.",
    .update = update_shorthand_extract_spec
};

sql_ctx_spec_t day_of_week_spec = {
    .name = "DAYOFWEEK",
    .description = "Returns the day of the week (0 for Sunday) from a DATETIME value.",
    .update = update_shorthand_extract_spec
};

sql_ctx_spec_t isodow_spec = {
    .name = "ISODOW",
    .description = "Returns the ISO day of the week (1 for Monday to 7 for Sunday) from a DATETIME value.",
    .update = update_shorthand_extract_spec
};

sql_ctx_spec_t iso_day_of_week_spec = {
    .name = "ISODAYOFWEEK",
    .description = "Returns the ISO day of the week (1 for Monday to 7 for Sunday) from a DATETIME value.",
    .update = update_shorthand_extract_spec
};


// Register EXTRACT function
void sql_register_extract(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &extract_spec);
    sql_ctx_register_spec(ctx, &datepart_spec);
    sql_ctx_register_spec(ctx, &year_spec);
    sql_ctx_register_spec(ctx, &month_spec);
    sql_ctx_register_spec(ctx, &day_spec);
    sql_ctx_register_spec(ctx, &hour_spec);
    sql_ctx_register_spec(ctx, &minute_spec);
    sql_ctx_register_spec(ctx, &second_spec);
    sql_ctx_register_spec(ctx, &quarter_spec);
    sql_ctx_register_spec(ctx, &week_spec);
    sql_ctx_register_spec(ctx, &doy_spec);
    sql_ctx_register_spec(ctx, &day_of_year_spec);
    sql_ctx_register_spec(ctx, &dow_spec);
    sql_ctx_register_spec(ctx, &day_of_week_spec);
    sql_ctx_register_spec(ctx, &isodow_spec);
    sql_ctx_register_spec(ctx, &iso_day_of_week_spec);


    // Register individual extraction functions
    sql_ctx_register_callback(ctx, sql_extract_year, "extract_year", "Extracts the year from a DATETIME.");
    sql_ctx_register_callback(ctx, sql_extract_month, "extract_month", "Extracts the month from a DATETIME.");
    sql_ctx_register_callback(ctx, sql_extract_day, "extract_day", "Extracts the day from a DATETIME.");
    sql_ctx_register_callback(ctx, sql_extract_hour, "extract_hour", "Extracts the hour from a DATETIME.");
    sql_ctx_register_callback(ctx, sql_extract_minute, "extract_minute", "Extracts the minute from a DATETIME.");
    sql_ctx_register_callback(ctx, sql_extract_second, "extract_second", "Extracts the second from a DATETIME.");
    sql_ctx_register_callback(ctx, sql_extract_quarter, "extract_quarter", "Extracts the quarter of the year from a DATETIME.");
    sql_ctx_register_callback(ctx, sql_extract_week, "extract_week", "Extracts the ISO week number from a DATETIME.");
    sql_ctx_register_callback(ctx, sql_extract_doy, "extract_doy", "Extracts the day of the year from a DATETIME.");
    sql_ctx_register_callback(ctx, sql_extract_dow, "extract_dow", "Extracts the day of the week (0 for Sunday) from a DATETIME.");
    sql_ctx_register_callback(ctx, sql_extract_isodow, "extract_isodow", "Extracts the ISO day of the week (1 for Monday to 7 for Sunday) from a DATETIME.");
}
