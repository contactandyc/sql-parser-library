// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_node.h"
#include <ctype.h>

static bool _sql_like(const char *value, const char *pattern) {
    if (!value || !pattern) {
        return false;
    }

    const char *v = value;
    const char *p = pattern;
    const char *v_star = NULL;  // Position in value to backtrack if needed
    const char *p_star = NULL;  // Position in pattern when encountering `%`

    while (*v) {
        if (*p == '%') {
            // Move past `%` and set backtracking positions
            p_star = ++p;
            v_star = v;
        } else if (*p == '_') {
            // `_` matches exactly one character
            if (!*v) return false;
            p++;
            v++;
        } else if (*p == *v) {
            // Direct character match
            p++;
            v++;
        } else if (p_star) {
            // Backtrack: reset pattern and move to the next character in value
            p = p_star;
            v = ++v_star;
        } else {
            // Mismatch without a `%` fallback
            return false;
        }
    }

    // Consume trailing `%` if present
    while (*p == '%') p++;

    return *p == '\0';  // Ensure full pattern is matched
}

static bool _sql_ilike(const char *value, const char *pattern) {
    if (!value || !pattern) {
        return false;
    }

    const char *v = value;
    const char *p = pattern;
    const char *v_star = NULL;  // Position in value to backtrack if needed
    const char *p_star = NULL;  // Position in pattern when encountering `%` or space

    while (*v) {
        // Treat both '%' and space as wildcards
        if (*p == '%' || *p == ' ') {
            p_star = ++p;
            v_star = v;
        } else if (*p == '_') {
            // '_' matches exactly one character
            if (!*v) return false;
            p++;
            v++;
        } else if (tolower((unsigned char)*p) == tolower((unsigned char)*v)) {
            // Direct character match (case insensitive)
            p++;
            v++;
        } else if (p_star) {
            // Backtrack: reset pattern and move to the next character in value
            p = p_star;
            v = ++v_star;
        } else {
            // Mismatch without a `%`/space fallback
            return false;
        }
    }

    // Consume any trailing '%' or spaces
    while (*p == '%' || *p == ' ') {
        p++;
    }

    return *p == '\0';  // Ensure the full pattern has been matched
}

// This is the old implementation of ILIKE, which is case insensitive but doesn't treat spaces as wildcards
static bool _sql_old_ilike(const char *value, const char *pattern) {
    if (!value || !pattern) {
        return false;
    }

    const char *v = value;
    const char *p = pattern;
    const char *v_star = NULL;  // Position in value to backtrack if needed
    const char *p_star = NULL;  // Position in pattern when encountering `%`

    while (*v) {
        if (*p == '%') {
            // Move past `%` and set backtracking positions
            p_star = ++p;
            v_star = v;
        } else if (*p == '_') {
            // `_` matches exactly one character
            if (!*v) return false;
            p++;
            v++;
        } else if (tolower((unsigned char)*p) == tolower((unsigned char)*v)) {
            // Direct character match (case insensitive)
            p++;
            v++;
        } else if (p_star) {
            // Backtrack: reset pattern and move to the next character in value
            p = p_star;
            v = ++v_star;
        } else {
            // Mismatch without a `%` fallback
            return false;
        }
    }

    // Consume trailing `%` if present
    while (*p == '%') p++;

    return *p == '\0';  // Ensure full pattern is matched
}

sql_node_t *sql_like(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *value = sql_eval(ctx, f->parameters[0]);
    sql_node_t *pattern = sql_eval(ctx, f->parameters[1]);
    if (!value || !pattern || value->is_null || pattern->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, _sql_ilike(value->value.string_value, pattern->value.string_value), false);
}

sql_node_t *sql_not_like(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *result = sql_like(ctx, f);
    if (!result) {
        return sql_bool_init(ctx, false, true);
    }
    result->value.bool_value = !result->value.bool_value;
    return result;
}

// Update function for LIKE
static sql_ctx_spec_update_t *update_like_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 2) {
        sql_ctx_error(ctx, "LIKE requires exactly two parameters.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 2;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, 2 * sizeof(sql_data_type_t));

    // Both parameters must be strings
    for (size_t i = 0; i < 2; i++) {
        if (f->parameters[i]->data_type != SQL_TYPE_STRING) {
            sql_ctx_error(ctx, "LIKE parameters must be of type STRING.");
            return NULL;
        }
        update->expected_data_types[i] = SQL_TYPE_STRING;
    }

    update->implementation = sql_like;
    update->return_type = SQL_TYPE_BOOL;

    return update;
}

// Update function for NOT LIKE
static sql_ctx_spec_update_t *update_not_like_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 2) {
        sql_ctx_error(ctx, "NOT LIKE requires exactly two parameters.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 2;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, 2 * sizeof(sql_data_type_t));

    // Both parameters must be strings
    for (size_t i = 0; i < 2; i++) {
        if (f->parameters[i]->data_type != SQL_TYPE_STRING) {
            sql_ctx_error(ctx, "NOT LIKE parameters must be of type STRING.");
            return NULL;
        }
        update->expected_data_types[i] = SQL_TYPE_STRING;
    }

    update->implementation = sql_not_like;
    update->return_type = SQL_TYPE_BOOL;

    return update;
}

// Specifications for LIKE and NOT LIKE
sql_ctx_spec_t like_function_spec = {
    .name = "LIKE",
    .description = "Checks if a value matches a pattern.",
    .update = update_like_spec
};

sql_ctx_spec_t not_like_function_spec = {
    .name = "NOT LIKE",
    .description = "Checks if a value does not match a pattern.",
    .update = update_not_like_spec
};

// Registration function
void sql_register_like(sql_ctx_t *ctx) {
    // Register the specifications
    sql_ctx_register_spec(ctx, &like_function_spec);
    sql_ctx_register_spec(ctx, &not_like_function_spec);

    // Register direct implementations for debugging or manual invocation
    sql_ctx_register_callback(ctx, sql_like, "like", "Check if value matches a pattern");
    sql_ctx_register_callback(ctx, sql_not_like, "not_like", "Check if value does not match a pattern");
}
