// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"
#include "a-memory-library/aml_pool.h"
#include "sql-parser-library/brutezone/timezone.h"
#include <string.h>

// Simplified Implementation of CONVERT_TZ
static sql_node_t *sql_convert_tz(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        sql_ctx_error(ctx, "CONVERT_TZ requires exactly two parameters: datetime, to_tz.");
        return sql_datetime_init(ctx, 0, true); // Return NULL
    }

    sql_node_t *datetime_node = sql_eval(ctx, f->parameters[0]);
    sql_node_t *to_tz_node = sql_eval(ctx, f->parameters[1]);

    if (!datetime_node || datetime_node->is_null || datetime_node->data_type != SQL_TYPE_DATETIME ||
        !to_tz_node || to_tz_node->is_null || to_tz_node->data_type != SQL_TYPE_STRING) {
        return sql_datetime_init(ctx, 0, true); // Return NULL if input is invalid
    }

    // Convert from UTC to the target timezone
    time_t local_time = timezone_local_time(to_tz_node->value.string_value, datetime_node->value.epoch);
    if (local_time < 0) {
        sql_ctx_error(ctx, "Invalid or ambiguous conversion to target timezone.");
        return sql_datetime_init(ctx, 0, true); // Return NULL
    }

    return sql_datetime_init(ctx, local_time, false);
}

// Update function for CONVERT_TZ
static sql_ctx_spec_update_t *update_convert_tz_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 2) {
        sql_ctx_error(ctx, "CONVERT_TZ requires exactly two parameters: datetime, to_tz.");
        return NULL;
    }

    // Allocate and initialize the update structure
    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));

    update->num_parameters = f->num_parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, sizeof(sql_data_type_t) * f->num_parameters);
    update->parameters = f->parameters;

    // Expect parameters to be of types (SQL_TYPE_DATETIME, SQL_TYPE_STRING)
    update->expected_data_types[0] = SQL_TYPE_DATETIME;
    update->expected_data_types[1] = SQL_TYPE_STRING;

    // Assign the implementation and return type
    update->implementation = sql_convert_tz;
    update->return_type = SQL_TYPE_DATETIME;

    return update;
}

// CONVERT_TZ function spec
static sql_ctx_spec_t convert_tz_function_spec = {
    .name = "CONVERT_TZ",
    .description = "Converts a datetime value from UTC to another timezone.",
    .update = update_convert_tz_spec
};

// Register CONVERT_TZ function
void sql_register_convert_tz(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &convert_tz_function_spec);

    sql_ctx_register_callback(ctx, sql_convert_tz, "convert_tz",
                              "Converts a datetime value from UTC to another timezone.");
}
