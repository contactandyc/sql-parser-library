// SPDX-FileCopyrightText: 2024-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai — technical questions: contact Andy (above)
// SPDX-License-Identifier: Apache-2.0

#include "sql-parser-library/sql_ctx.h"
#include <time.h>

// Implementation for NOW and CURRENT_TIMESTAMP
static sql_node_t *sql_func_now(sql_ctx_t *ctx, sql_node_t *f) {
    time_t now = time(NULL);
    return sql_datetime_init(ctx, now, false);
}

// Implementation for CURRENT_DATE
static sql_node_t *sql_func_current_date(sql_ctx_t *ctx, sql_node_t *f) {
    time_t now = time(NULL);
    struct tm tm_info;
    gmtime_r(&now, &tm_info);
    tm_info.tm_hour = 0;
    tm_info.tm_min = 0;
    tm_info.tm_sec = 0;

    time_t date_epoch = timegm(&tm_info);
    return sql_datetime_init(ctx, date_epoch, false);
}

// Specification Update Functions

// Update function for NOW, GETDATE, and CURRENT_TIMESTAMP
static sql_ctx_spec_update_t *update_now_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 0; // No parameters expected
    update->parameters = NULL; // No parameters to evaluate
    update->expected_data_types = NULL; // No expected types
    update->implementation = sql_func_now; // Assign the correct implementation
    update->return_type = SQL_TYPE_DATETIME; // The return type is DATETIME
    return update;
}

// Update function for CURRENT_DATE
static sql_ctx_spec_update_t *update_current_date_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 0; // No parameters expected
    update->parameters = NULL; // No parameters to evaluate
    update->expected_data_types = NULL; // No expected types
    update->implementation = sql_func_current_date; // Assign the correct implementation
    update->return_type = SQL_TYPE_DATETIME; // The return type is DATETIME
    return update;
}

// Function Specifications
sql_ctx_spec_t now_function_spec = {
    .name = "NOW",
    .description = "Returns the current date and time.",
    .update = update_now_spec
};

sql_ctx_spec_t getdate_function_spec = {
    .name = "GETDATE",
    .description = "Returns the current date and time (DATETIME).",
    .update = update_now_spec
};

sql_ctx_spec_t current_date_function_spec = {
    .name = "CURRENT_DATE",
    .description = "Returns the current date (DATE).",
    .update = update_current_date_spec
};

sql_ctx_spec_t current_timestamp_function_spec = {
    .name = "CURRENT_TIMESTAMP",
    .description = "Returns the current date and time (DATETIME).",
    .update = update_now_spec
};

// Registration Function
void sql_register_now(sql_ctx_t *ctx) {
    // Register function specifications
    sql_ctx_register_spec(ctx, &now_function_spec);
    sql_ctx_register_spec(ctx, &getdate_function_spec);
    sql_ctx_register_spec(ctx, &current_date_function_spec);
    sql_ctx_register_spec(ctx, &current_timestamp_function_spec);

    // Optionally register the individual function implementations for debugging or direct calls
    sql_ctx_register_callback(ctx, sql_func_now, "sql_func_now", "Returns the current date and time.");
    sql_ctx_register_callback(ctx, sql_func_current_date, "sql_func_current_date", "Returns the current date.");
}
