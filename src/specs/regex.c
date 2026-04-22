// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"
#include <regex.h>

static sql_node_t *sql_func_regexp_like(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *str_node = sql_eval(ctx, f->parameters[0]);
    sql_node_t *pattern_node = sql_eval(ctx, f->parameters[1]);

    if (!str_node || str_node->is_null || !pattern_node || pattern_node->is_null) {
        return sql_bool_init(ctx, false, true);
    }

    regex_t regex;
    // Compile regex (extended POSIX)
    int reti = regcomp(&regex, pattern_node->value.string_value, REG_EXTENDED | REG_NOSUB);
    if (reti) {
        return sql_bool_init(ctx, false, false); // Invalid pattern compiles as false
    }

    reti = regexec(&regex, str_node->value.string_value, 0, NULL, 0);
    regfree(&regex);

    // regexec returns 0 for a successful match
    return sql_bool_init(ctx, !reti, false);
}

static sql_ctx_spec_update_t *update_regexp_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 2) return NULL;
    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 2;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, 2 * sizeof(sql_data_type_t));
    update->expected_data_types[0] = SQL_TYPE_STRING;
    update->expected_data_types[1] = SQL_TYPE_STRING;
    update->return_type = SQL_TYPE_BOOL;
    update->implementation = sql_func_regexp_like;
    return update;
}

sql_ctx_spec_t regexp_like_spec = { .name = "REGEXP_LIKE", .description = "Regex match", .update = update_regexp_spec };
sql_ctx_spec_t regexp_op_spec = { .name = "~", .description = "Regex operator", .update = update_regexp_spec };

void sql_register_regex(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &regexp_like_spec);
    sql_ctx_register_spec(ctx, &regexp_op_spec);
}
