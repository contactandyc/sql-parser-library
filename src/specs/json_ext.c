// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"
#include "a-memory-library/aml_pool.h"
#include "a-json-library/ajson.h"
#include <string.h>

static sql_node_t *sql_func_json_extract(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *json_node = sql_eval(ctx, f->parameters[0]);
    sql_node_t *key_node = sql_eval(ctx, f->parameters[1]);

    if (!json_node || json_node->is_null || !key_node || key_node->is_null) {
        return sql_string_init(ctx, NULL, true);
    }

    const char *json_text = json_node->value.string_value;
    const char *path_text = key_node->value.string_value;

    // 1. Parse the raw JSON text into an ajson tree using the context's memory pool
    ajson_t *parsed_json = ajson_parse_string(ctx->pool, json_text);
    if (!parsed_json || ajson_is_error(parsed_json)) {
        return sql_string_init(ctx, NULL, true);
    }

    // 2. Clean up standard SQL JSON pathing.
    // If the path starts with "$.", strip it so ajsono_path doesn't look for a key named "$"
    if (path_text[0] == '$' && path_text[1] == '.') {
        path_text += 2;
    }
    // PostgreSQL ->> often just passes 'browser' without the '$.'. If it's just 'browser',
    // path_text is already perfectly formatted for ajsono_path.

    // 3. Use ajson's native path navigator to find the node
    ajson_t *extracted_node = ajsono_path(ctx->pool, parsed_json, path_text);
    if (!extracted_node || ajson_is_error(extracted_node) || ajson_is_null(extracted_node)) {
        return sql_string_init(ctx, NULL, true);
    }

    // 4. Extract the underlying value.
    // We use ajsond to decode strings, and ajson_to_str for numbers/booleans
    const char *result_str = NULL;
    if (ajson_is_string(extracted_node)) {
        result_str = ajsond(ctx->pool, extracted_node);
    } else {
        result_str = ajson_to_str(extracted_node, NULL);
    }

    if (!result_str) {
        return sql_string_init(ctx, NULL, true);
    }

    return sql_string_init(ctx, result_str, false);
}

static sql_ctx_spec_update_t *update_json_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 2) {
        sql_ctx_error(ctx, "%s requires exactly 2 parameters: json_string, path.", spec->name);
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 2;
    update->parameters = f->parameters;

    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, 2 * sizeof(sql_data_type_t));
    update->expected_data_types[0] = SQL_TYPE_STRING; // The JSON string
    update->expected_data_types[1] = SQL_TYPE_STRING; // The path/key

    update->return_type = SQL_TYPE_STRING;
    update->implementation = sql_func_json_extract;

    return update;
}

// -----------------------------------------------------------------------------
// REGISTRATION
// -----------------------------------------------------------------------------

sql_ctx_spec_t json_extract_spec = {
    .name = "JSON_EXTRACT",
    .description = "Extracts data from a JSON string using a JSON path.",
    .update = update_json_spec
};

sql_ctx_spec_t json_op_spec = {
    .name = "->>",
    .description = "Extracts data from a JSON string using a JSON path (PostgreSQL style).",
    .update = update_json_spec
};

void sql_register_json(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &json_extract_spec);
    sql_ctx_register_spec(ctx, &json_op_spec);

    sql_ctx_register_callback(ctx, sql_func_json_extract, "json_extract", "JSON_EXTRACT function");
}
