// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_node.h"
#include "sql-parser-library/sql_ctx.h"
#include <math.h>
#include <string.h>

// -----------------------------------------------------------------------------
// MODULO (%)
// -----------------------------------------------------------------------------
static sql_node_t *sql_int_modulo(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null || right->value.int_value == 0) {
        return sql_int_init(ctx, 0, true); // NULL on div-by-zero
    }
    return sql_int_init(ctx, left->value.int_value % right->value.int_value, false);
}

static sql_node_t *sql_double_modulo(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null || right->value.double_value == 0.0) {
        return sql_double_init(ctx, 0.0, true); // NULL on div-by-zero
    }
    return sql_double_init(ctx, fmod(left->value.double_value, right->value.double_value), false);
}

// -----------------------------------------------------------------------------
// EXPONENT (^)
// -----------------------------------------------------------------------------
static sql_node_t *sql_int_exponent(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_int_init(ctx, 0, true);
    }
    int result = (int)pow((double)left->value.int_value, (double)right->value.int_value);
    return sql_int_init(ctx, result, false);
}

static sql_node_t *sql_double_exponent(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_double_init(ctx, 0.0, true);
    }
    double result = pow(left->value.double_value, right->value.double_value);
    return sql_double_init(ctx, result, false);
}

// -----------------------------------------------------------------------------
// BITWISE OPERATORS (&, |, <<, >>, ~)
// -----------------------------------------------------------------------------
static sql_node_t *sql_int_bitwise_and(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) return sql_int_init(ctx, 0, true);
    return sql_int_init(ctx, left->value.int_value & right->value.int_value, false);
}

static sql_node_t *sql_int_bitwise_or(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) return sql_int_init(ctx, 0, true);
    return sql_int_init(ctx, left->value.int_value | right->value.int_value, false);
}

static sql_node_t *sql_int_bitwise_shift_left(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) return sql_int_init(ctx, 0, true);
    return sql_int_init(ctx, left->value.int_value << right->value.int_value, false);
}

static sql_node_t *sql_int_bitwise_shift_right(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) return sql_int_init(ctx, 0, true);
    return sql_int_init(ctx, left->value.int_value >> right->value.int_value, false);
}

static sql_node_t *sql_int_bitwise_not(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) return sql_int_init(ctx, 0, true);
    return sql_int_init(ctx, ~child->value.int_value, false);
}

// -----------------------------------------------------------------------------
// SPEC UPDATES
// -----------------------------------------------------------------------------

// Used for % and ^
static sql_ctx_spec_update_t *update_math_ext_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 2) {
        sql_ctx_error(ctx, "Operator %s requires exactly two parameters.", spec->name);
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 2;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, 2 * sizeof(sql_data_type_t));

    // Promote to double if mixed
    sql_data_type_t data_type = f->parameters[0]->data_type;
    if (f->parameters[0]->data_type != f->parameters[1]->data_type) {
        if ((f->parameters[0]->data_type == SQL_TYPE_INT && f->parameters[1]->data_type == SQL_TYPE_DOUBLE) ||
            (f->parameters[0]->data_type == SQL_TYPE_DOUBLE && f->parameters[1]->data_type == SQL_TYPE_INT)) {
            data_type = SQL_TYPE_DOUBLE;
        }
    }

    update->expected_data_types[0] = data_type;
    update->expected_data_types[1] = data_type;
    update->return_type = data_type;

    if (data_type == SQL_TYPE_INT) {
        if (strcmp(spec->name, "%") == 0) update->implementation = sql_int_modulo;
        else if (strcmp(spec->name, "^") == 0) update->implementation = sql_int_exponent;
    } else if (data_type == SQL_TYPE_DOUBLE) {
        if (strcmp(spec->name, "%") == 0) update->implementation = sql_double_modulo;
        else if (strcmp(spec->name, "^") == 0) update->implementation = sql_double_exponent;
    } else {
        sql_ctx_error(ctx, "Operator %s not supported for data type %s.", spec->name, sql_data_type_name(data_type));
        return NULL;
    }
    return update;
}

// Used for &, |, <<, >>
static sql_ctx_spec_update_t *update_bitwise_binary_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 2) {
        sql_ctx_error(ctx, "Bitwise operator %s requires exactly two parameters.", spec->name);
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 2;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, 2 * sizeof(sql_data_type_t));

    // Bitwise operators ONLY work on integers in C.
    // This forces coercion down to INT if they passed doubles.
    update->expected_data_types[0] = SQL_TYPE_INT;
    update->expected_data_types[1] = SQL_TYPE_INT;
    update->return_type = SQL_TYPE_INT;

    if (strcmp(spec->name, "&") == 0) update->implementation = sql_int_bitwise_and;
    else if (strcmp(spec->name, "|") == 0) update->implementation = sql_int_bitwise_or;
    else if (strcmp(spec->name, "<<") == 0) update->implementation = sql_int_bitwise_shift_left;
    else if (strcmp(spec->name, ">>") == 0) update->implementation = sql_int_bitwise_shift_right;

    return update;
}

// Used for ~
static sql_ctx_spec_update_t *update_bitwise_unary_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 1) {
        sql_ctx_error(ctx, "Bitwise NOT (~) requires exactly one parameter.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 1;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, sizeof(sql_data_type_t));

    update->expected_data_types[0] = SQL_TYPE_INT;
    update->return_type = SQL_TYPE_INT;
    update->implementation = sql_int_bitwise_not;

    return update;
}

// -----------------------------------------------------------------------------
// REGISTRATION
// -----------------------------------------------------------------------------

static sql_ctx_spec_t modulo_spec       = { .name = "%",  .description = "Modulo", .update = update_math_ext_spec };
static sql_ctx_spec_t exponent_spec     = { .name = "^",  .description = "Exponent", .update = update_math_ext_spec };
static sql_ctx_spec_t bitwise_and_spec  = { .name = "&",  .description = "Bitwise AND", .update = update_bitwise_binary_spec };
static sql_ctx_spec_t bitwise_or_spec   = { .name = "|",  .description = "Bitwise OR", .update = update_bitwise_binary_spec };
static sql_ctx_spec_t bitwise_shl_spec  = { .name = "<<", .description = "Bitwise Shift Left", .update = update_bitwise_binary_spec };
static sql_ctx_spec_t bitwise_shr_spec  = { .name = ">>", .description = "Bitwise Shift Right", .update = update_bitwise_binary_spec };
static sql_ctx_spec_t bitwise_not_spec  = { .name = "~",  .description = "Bitwise NOT", .update = update_bitwise_unary_spec };

void sql_register_arithmetic_ext(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &modulo_spec);
    sql_ctx_register_spec(ctx, &exponent_spec);
    sql_ctx_register_spec(ctx, &bitwise_and_spec);
    sql_ctx_register_spec(ctx, &bitwise_or_spec);
    sql_ctx_register_spec(ctx, &bitwise_shl_spec);
    sql_ctx_register_spec(ctx, &bitwise_shr_spec);
    sql_ctx_register_spec(ctx, &bitwise_not_spec);

    sql_ctx_register_callback(ctx, sql_int_modulo, "int_modulo", "INT % INT");
    sql_ctx_register_callback(ctx, sql_double_modulo, "double_modulo", "DOUBLE % DOUBLE");
    sql_ctx_register_callback(ctx, sql_int_exponent, "int_exponent", "INT ^ INT");
    sql_ctx_register_callback(ctx, sql_double_exponent, "double_exponent", "DOUBLE ^ DOUBLE");
    sql_ctx_register_callback(ctx, sql_int_bitwise_and, "int_bitwise_and", "INT & INT");
    sql_ctx_register_callback(ctx, sql_int_bitwise_or, "int_bitwise_or", "INT | INT");
    sql_ctx_register_callback(ctx, sql_int_bitwise_shift_left, "int_bitwise_shl", "INT << INT");
    sql_ctx_register_callback(ctx, sql_int_bitwise_shift_right, "int_bitwise_shr", "INT >> INT");
    sql_ctx_register_callback(ctx, sql_int_bitwise_not, "int_bitwise_not", "~INT");
}
