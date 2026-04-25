// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_node.h"
#include "sql-parser-library/sql_ctx.h"

// Determine common type for IN expressions
static sql_data_type_t determine_common_type(sql_data_type_t type1, sql_data_type_t type2) {
    if (type1 == type2) return type1;
    if ((type1 == SQL_TYPE_INT && type2 == SQL_TYPE_DOUBLE) ||
        (type1 == SQL_TYPE_DOUBLE && type2 == SQL_TYPE_INT))
        return SQL_TYPE_DOUBLE;
    return SQL_TYPE_STRING; // Default promotion to string for mismatched types
}

// Evaluate IN for integers
static sql_node_t *sql_int_in(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2 || f->parameters[1]->type != SQL_LIST) {
        return sql_bool_init(ctx, false, true);
    }

    sql_node_t *value = sql_eval(ctx, f->parameters[0]);
    sql_node_t *list = f->parameters[1];

    if (!value || value->is_null || !list) return sql_bool_init(ctx, false, true);

    int target = value->value.int_value;
    bool found = false, has_null = false;

    for (size_t i = 0; i < list->num_parameters; i++) {
        sql_node_t *elem = sql_eval(ctx, list->parameters[i]);
        if (!elem || elem->is_null) {
            has_null = true;
            continue;
        }
        if (elem->value.int_value == target) {
            found = true;
            break;
        }
    }

    return sql_bool_init(ctx, found, !found && has_null);
}

// Evaluate IN for doubles
static sql_node_t *sql_double_in(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2 || f->parameters[1]->type != SQL_LIST) {
        return sql_bool_init(ctx, false, true);
    }

    sql_node_t *value = sql_eval(ctx, f->parameters[0]);
    sql_node_t *list = f->parameters[1];

    if (!value || value->is_null || !list) return sql_bool_init(ctx, false, true);

    double target = value->value.double_value;
    bool found = false, has_null = false;

    for (size_t i = 0; i < list->num_parameters; i++) {
        sql_node_t *elem = sql_eval(ctx, list->parameters[i]);
        if (!elem || elem->is_null) {
            has_null = true;
            continue;
        }
        if (elem->value.double_value == target) {
            found = true;
            break;
        }
    }

    return sql_bool_init(ctx, found, !found && has_null);
}

// Evaluate IN for strings
static sql_node_t *sql_string_in(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2 || f->parameters[1]->type != SQL_LIST) {
        return sql_bool_init(ctx, false, true);
    }

    sql_node_t *value = sql_eval(ctx, f->parameters[0]);
    sql_node_t *list = f->parameters[1];

    if (!value || value->is_null || !list) return sql_bool_init(ctx, false, true);

    char *target = (char *)value->value.string_value;
    bool found = false, has_null = false;

    for (size_t i = 0; i < list->num_parameters; i++) {
        sql_node_t *elem = sql_eval(ctx, list->parameters[i]);
        if (!elem || elem->is_null) {
            has_null = true;
            continue;
        }
        if (strcasecmp(elem->value.string_value, target) == 0) {
            found = true;
            break;
        }
    }

    return sql_bool_init(ctx, found, !found && has_null);
}

static sql_node_t *sql_int_not_in(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *in_result = sql_int_in(ctx, f);
    // TODO: This deviates from the SQL standard - but works generally how an LLM might expect.  If NULL and false,
    //    then it is treated as NOT IN.   Normally NULL on IN should also be NULL for NOT IN.
    if (in_result->is_null && !in_result->value.bool_value) sql_bool_init(ctx, true, false); // return in_result;
    return sql_bool_init(ctx, !in_result->value.bool_value, false);
}

// Evaluate NOT IN for doubles
static sql_node_t *sql_double_not_in(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *in_result = sql_double_in(ctx, f);
    if (in_result->is_null && !in_result->value.bool_value) sql_bool_init(ctx, true, false); // return in_result;
    return sql_bool_init(ctx, !in_result->value.bool_value, false);
}

// Evaluate NOT IN for strings
static sql_node_t *sql_string_not_in(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *in_result = sql_string_in(ctx, f);
    if (in_result->is_null && !in_result->value.bool_value) sql_bool_init(ctx, true, false); // return in_result;
    return sql_bool_init(ctx, !in_result->value.bool_value, false);
}

static sql_ctx_spec_update_t *update_in_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 2) {
        sql_ctx_error(ctx, "IN requires exactly two parameters: a value and a list.");
        return NULL;
    }

    sql_node_t *value = f->parameters[0];
    sql_node_t *list = f->parameters[1];

    if (list->type != SQL_LIST || (list->type == SQL_FUNCTION && !strcmp(list->token, "CONVERT") && list->parameters[0]->type == SQL_LIST)) {
        sql_ctx_error(ctx, "The second parameter of IN must be a list.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 2;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, 2 * sizeof(sql_data_type_t));

    // Infer common type between the value and the list elements
    sql_data_type_t common_type = value->data_type;
    for (size_t i = 0; i < list->num_parameters; i++) {
        common_type = determine_common_type(common_type, list->parameters[i]->data_type);
    }

    // Set expected types
    update->expected_data_types[0] = common_type;
    update->expected_data_types[1] = common_type;

    // Assign correct function
    switch (common_type) {
        case SQL_TYPE_INT:
            update->implementation = sql_int_in;
            break;
        case SQL_TYPE_DOUBLE:
            update->implementation = sql_double_in;
            break;
        case SQL_TYPE_STRING:
            update->implementation = sql_string_in;
            break;
        default:
            sql_ctx_error(ctx, "IN is not supported for this type.");
            return NULL;
    }
    update->return_type = SQL_TYPE_BOOL;
    return update;
}

static sql_ctx_spec_update_t *update_not_in_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 2) {
        sql_ctx_error(ctx, "NOT IN requires exactly two parameters: a value and a list.");
        return NULL;
    }

    sql_node_t *value = f->parameters[0];
    sql_node_t *list = f->parameters[1];

    if (list->type != SQL_LIST || (list->type == SQL_FUNCTION && !strcmp(list->token, "CONVERT") && list->parameters[0]->type == SQL_LIST)) {
        sql_ctx_error(ctx, "The second parameter of NOT IN must be a list.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 2;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, 2 * sizeof(sql_data_type_t));

    // Infer common type between the value and the list elements
    sql_data_type_t common_type = value->data_type;
    for (size_t i = 0; i < list->num_parameters; i++) {
        common_type = determine_common_type(common_type, list->parameters[i]->data_type);
    }

    // Set expected types
    update->expected_data_types[0] = common_type;
    update->expected_data_types[1] = common_type;

    // Assign correct NOT IN function based on type
    switch (common_type) {
        case SQL_TYPE_INT:
            update->implementation = sql_int_not_in;
            break;
        case SQL_TYPE_DOUBLE:
            update->implementation = sql_double_not_in;
            break;
        case SQL_TYPE_STRING:
            update->implementation = sql_string_not_in;
            break;
        default:
            sql_ctx_error(ctx, "NOT IN is not supported for this type.");
            return NULL;
    }
    update->return_type = SQL_TYPE_BOOL;
    return update;
}


// Specifications for IN and NOT IN
static sql_ctx_spec_t in_function_spec = {
    .name = "IN",
    .description = "Checks if a value is in a list (supports type promotion).",
    .update = update_in_spec
};

static sql_ctx_spec_t not_in_function_spec = {
    .name = "NOT IN",
    .description = "Checks if a value is not in a list (supports type promotion).",
    .update = update_not_in_spec
};

// Registration function
void sql_register_in(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &in_function_spec);
    sql_ctx_register_spec(ctx, &not_in_function_spec);

    sql_ctx_register_callback(ctx, sql_int_in, "int_in", "Check if an integer is in a list");
    sql_ctx_register_callback(ctx, sql_double_in, "double_in", "Check if a double is in a list");
    sql_ctx_register_callback(ctx, sql_string_in, "string_in", "Check if a string is in a list");

    sql_ctx_register_callback(ctx, sql_int_not_in, "int_not_in", "Check if an integer is NOT in a list");
    sql_ctx_register_callback(ctx, sql_double_not_in, "double_not_in", "Check if a double is NOT in a list");
    sql_ctx_register_callback(ctx, sql_string_not_in, "string_not_in", "Check if a string is NOT in a list");
}
