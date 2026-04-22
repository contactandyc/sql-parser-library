// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"
#include <string.h>

static sql_node_t *sql_func_case(sql_ctx_t *ctx, sql_node_t *f) {
    for (size_t i = 0; i < f->num_parameters; i++) {
        sql_node_t *clause = f->parameters[i];
        if (strcasecmp(clause->token, "WHEN") == 0) {
            sql_node_t *cond = sql_eval(ctx, clause->parameters[0]);
            if (cond && !cond->is_null && cond->value.bool_value) {
                return sql_eval(ctx, clause->parameters[1]);
            }
        } else if (strcasecmp(clause->token, "ELSE") == 0) {
            return sql_eval(ctx, clause->parameters[0]);
        }
    }
    return sql_string_init(ctx, NULL, true); // Fallback if no ELSE provided
}

static sql_ctx_spec_update_t *update_case_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = f->num_parameters;
    update->parameters = f->parameters;

    // Determine target return type based on THEN/ELSE outputs
    sql_data_type_t return_type = SQL_TYPE_UNKNOWN;
    for (size_t i = 0; i < f->num_parameters; i++) {
        sql_node_t *clause = f->parameters[i];
        if (strcasecmp(clause->token, "WHEN") == 0 && clause->num_parameters == 2) {
            if (return_type == SQL_TYPE_UNKNOWN) return_type = clause->parameters[1]->data_type;
        } else if (strcasecmp(clause->token, "ELSE") == 0 && clause->num_parameters == 1) {
            if (return_type == SQL_TYPE_UNKNOWN) return_type = clause->parameters[0]->data_type;
        }
    }

    // Coerce everything to match
    for (size_t i = 0; i < f->num_parameters; i++) {
        sql_node_t *clause = f->parameters[i];
        if (strcasecmp(clause->token, "WHEN") == 0) {
            if (clause->parameters[0]->data_type != SQL_TYPE_BOOL) {
                clause->parameters[0] = sql_convert(ctx, clause->parameters[0], SQL_TYPE_BOOL);
            }
            if (clause->parameters[1]->data_type != return_type) {
                clause->parameters[1] = sql_convert(ctx, clause->parameters[1], return_type);
            }
        } else if (strcasecmp(clause->token, "ELSE") == 0) {
            if (clause->parameters[0]->data_type != return_type) {
                clause->parameters[0] = sql_convert(ctx, clause->parameters[0], return_type);
            }
        }
    }

    update->return_type = return_type == SQL_TYPE_UNKNOWN ? SQL_TYPE_STRING : return_type;
    update->implementation = sql_func_case;
    return update;
}

sql_ctx_spec_t case_spec = { .name = "CASE", .description = "CASE evaluation", .update = update_case_spec };

void sql_register_case(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &case_spec);
}
