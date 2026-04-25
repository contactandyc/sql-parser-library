// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"
#include "a-memory-library/aml_pool.h"
#include <string.h>

// -----------------------------------------------------------------------------
// REPLACE(string, search, replace)
// -----------------------------------------------------------------------------
static sql_node_t *sql_func_replace(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *str_node = sql_eval(ctx, f->parameters[0]);
    sql_node_t *search_node = sql_eval(ctx, f->parameters[1]);
    sql_node_t *replace_node = sql_eval(ctx, f->parameters[2]);

    // If any parameter is NULL, SQL standard dictates returning NULL
    if (!str_node || str_node->is_null ||
        !search_node || search_node->is_null ||
        !replace_node || replace_node->is_null) {
        return sql_string_init(ctx, NULL, true);
    }

    const char *s = str_node->value.string_value;
    const char *sub = search_node->value.string_value;
    const char *rep = replace_node->value.string_value;

    size_t sub_len = strlen(sub);
    size_t rep_len = strlen(rep);

    // If search string is empty, return original string
    if (sub_len == 0) {
        return sql_string_init(ctx, aml_pool_strdup(ctx->pool, s), false);
    }

    // Count occurrences to allocate the exact right amount of memory
    int count = 0;
    const char *tmp = s;
    while ((tmp = strstr(tmp, sub))) {
        count++;
        tmp += sub_len;
    }

    // Allocate memory from the memory pool
    size_t new_len = strlen(s) + count * (rep_len - sub_len);
    char *result = aml_pool_alloc(ctx->pool, new_len + 1);
    char *out = result;

    // Perform the replacements
    tmp = s;
    const char *match;
    while ((match = strstr(tmp, sub))) {
        size_t len = match - tmp;
        memcpy(out, tmp, len);
        out += len;
        memcpy(out, rep, rep_len);
        out += rep_len;
        tmp = match + sub_len;
    }
    strcpy(out, tmp); // Copy the remaining part of the string

    return sql_string_init(ctx, result, false);
}

// -----------------------------------------------------------------------------
// SPLIT_PART(string, delimiter, position)
// -----------------------------------------------------------------------------
static sql_node_t *sql_func_split_part(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *str_node = sql_eval(ctx, f->parameters[0]);
    sql_node_t *delim_node = sql_eval(ctx, f->parameters[1]);
    sql_node_t *index_node = sql_eval(ctx, f->parameters[2]);

    if (!str_node || str_node->is_null ||
        !delim_node || delim_node->is_null ||
        !index_node || index_node->is_null) {
        return sql_string_init(ctx, NULL, true);
    }

    const char *s = str_node->value.string_value;
    const char *delim = delim_node->value.string_value;
    int target_idx = index_node->value.int_value;
    size_t delim_len = strlen(delim);

    // SQL indices are 1-based. If index < 1 or delimiter is empty, return empty string
    if (target_idx < 1 || delim_len == 0) {
        return sql_string_init(ctx, "", false);
    }

    int current_idx = 1;
    const char *start = s;
    const char *end;

    // Iterate through the string finding the delimiter
    while ((end = strstr(start, delim))) {
        if (current_idx == target_idx) {
            size_t len = end - start;
            char *res = aml_pool_alloc(ctx->pool, len + 1);
            memcpy(res, start, len);
            res[len] = '\0';
            return sql_string_init(ctx, res, false);
        }
        start = end + delim_len;
        current_idx++;
    }

    // Capture the final part if it is the target index
    if (current_idx == target_idx) {
        return sql_string_init(ctx, aml_pool_strdup(ctx->pool, start), false);
    }

    // If the index was out of bounds, return an empty string
    return sql_string_init(ctx, "", false);
}

// -----------------------------------------------------------------------------
// POSITION(substring IN string)
// -----------------------------------------------------------------------------
static sql_node_t *sql_func_position(sql_ctx_t *ctx, sql_node_t *f) {
    // Thanks to the AST hack, param[0] is the substring, param[1] is the main string
    sql_node_t *substr_node = sql_eval(ctx, f->parameters[0]);
    sql_node_t *str_node = sql_eval(ctx, f->parameters[1]);

    if (!str_node || str_node->is_null || !substr_node || substr_node->is_null) {
        return sql_int_init(ctx, 0, true);
    }

    const char *sub = substr_node->value.string_value;
    const char *s = str_node->value.string_value;

    char *match = strstr(s, sub);
    if (match) {
        // SQL strings are 1-indexed
        int pos = (int)(match - s) + 1;
        return sql_int_init(ctx, pos, false);
    }

    // Substring not found returns 0 in SQL
    return sql_int_init(ctx, 0, false);
}

// -----------------------------------------------------------------------------
// SPEC UPDATES
// -----------------------------------------------------------------------------
static sql_ctx_spec_update_t *update_replace_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 3) {
        sql_ctx_error(ctx, "REPLACE requires exactly 3 parameters: string, search, replace.");
        return NULL;
    }
    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 3;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, 3 * sizeof(sql_data_type_t));

    update->expected_data_types[0] = SQL_TYPE_STRING;
    update->expected_data_types[1] = SQL_TYPE_STRING;
    update->expected_data_types[2] = SQL_TYPE_STRING;

    update->return_type = SQL_TYPE_STRING;
    update->implementation = sql_func_replace;
    return update;
}

static sql_ctx_spec_update_t *update_split_part_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 3) {
        sql_ctx_error(ctx, "SPLIT_PART requires exactly 3 parameters: string, delimiter, position.");
        return NULL;
    }
    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 3;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, 3 * sizeof(sql_data_type_t));

    update->expected_data_types[0] = SQL_TYPE_STRING;
    update->expected_data_types[1] = SQL_TYPE_STRING;
    update->expected_data_types[2] = SQL_TYPE_INT; // Position must be an integer

    update->return_type = SQL_TYPE_STRING;
    update->implementation = sql_func_split_part;
    return update;
}

static sql_ctx_spec_update_t *update_position_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 2) {
        sql_ctx_error(ctx, "POSITION requires exactly 2 parameters.");
        return NULL;
    }
    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 2;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, 2 * sizeof(sql_data_type_t));

    update->expected_data_types[0] = SQL_TYPE_STRING;
    update->expected_data_types[1] = SQL_TYPE_STRING;

    update->return_type = SQL_TYPE_INT;
    update->implementation = sql_func_position;
    return update;
}

// -----------------------------------------------------------------------------
// REGISTRATION
// -----------------------------------------------------------------------------
static sql_ctx_spec_t replace_spec = {
    .name = "REPLACE",
    .description = "Replaces all occurrences of a substring within a string.",
    .update = update_replace_spec
};

static sql_ctx_spec_t split_part_spec = {
    .name = "SPLIT_PART",
    .description = "Splits a string on a delimiter and returns the Nth part (1-indexed).",
    .update = update_split_part_spec
};

static sql_ctx_spec_t position_spec = {
    .name = "POSITION",
    .description = "Returns the position of the first occurrence of a substring in a string.",
    .update = update_position_spec
};

void sql_register_string_ext(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &replace_spec);
    sql_ctx_register_spec(ctx, &split_part_spec);
    sql_ctx_register_spec(ctx, &position_spec);

    sql_ctx_register_callback(ctx, sql_func_replace, "replace", "REPLACE function");
    sql_ctx_register_callback(ctx, sql_func_split_part, "split_part", "SPLIT_PART function");
    sql_ctx_register_callback(ctx, sql_func_position, "position", "POSITION function");
}
