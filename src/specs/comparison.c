// SPDX-FileCopyrightText: 2024-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai — technical questions: contact Andy (above)
// SPDX-License-Identifier: Apache-2.0

#include "sql-parser-library/sql_node.h"
#include "sql-parser-library/sql_ctx.h"
#include <strings.h>

sql_node_t *sql_bool_less(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, left->value.bool_value < right->value.bool_value, false);
}

sql_node_t *sql_bool_less_or_equal(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, left->value.bool_value <= right->value.bool_value, false);
}

sql_node_t *sql_bool_not_equal(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, left->value.bool_value != right->value.bool_value, false);
}

sql_node_t *sql_bool_equal(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, left->value.bool_value == right->value.bool_value, false);
}

sql_node_t *sql_int_less(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, left->value.int_value < right->value.int_value, false);
}

sql_node_t *sql_int_less_or_equal(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, left->value.int_value <= right->value.int_value, false);
}

sql_node_t *sql_int_not_equal(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, left->value.int_value != right->value.int_value, false);
}

sql_node_t *sql_int_equal(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, left->value.int_value == right->value.int_value, false);
}

sql_node_t *sql_double_less(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, left->value.double_value < right->value.double_value, false);
}

sql_node_t *sql_double_less_or_equal(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, left->value.double_value <= right->value.double_value, false);
}

sql_node_t *sql_double_not_equal(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, left->value.double_value != right->value.double_value, false);
}

sql_node_t *sql_double_equal(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, left->value.double_value == right->value.double_value, false);
}

sql_node_t *sql_string_less(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    int result = strcasecmp(left->value.string_value, right->value.string_value);
    return sql_bool_init(ctx, result < 0, false);
}

sql_node_t *sql_string_less_or_equal(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    int result = strcasecmp(left->value.string_value, right->value.string_value);
    return sql_bool_init(ctx, result <= 0, false);
}

sql_node_t *sql_string_not_equal(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    int result = strcasecmp(left->value.string_value, right->value.string_value);
    return sql_bool_init(ctx, result != 0, false);
}

sql_node_t *sql_string_equal(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    int result = strcasecmp(left->value.string_value, right->value.string_value);
    return sql_bool_init(ctx, result == 0, false);
}

sql_node_t *sql_datetime_less(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, left->value.epoch < right->value.epoch, false);
}

sql_node_t *sql_datetime_less_or_equal(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, left->value.epoch <= right->value.epoch, false);
}

sql_node_t *sql_datetime_not_equal(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, left->value.epoch != right->value.epoch, false);
}

sql_node_t *sql_datetime_equal(sql_ctx_t *ctx, sql_node_t *f) {
    if (f->num_parameters != 2) {
        return sql_bool_init(ctx, false, true);
    }
    sql_node_t *left = sql_eval(ctx, f->parameters[0]);
    sql_node_t *right = sql_eval(ctx, f->parameters[1]);
    if (!left || !right || left->is_null || right->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, left->value.epoch == right->value.epoch, false);
}

static sql_ctx_spec_update_t *update_less_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 2) {
        sql_ctx_error(ctx, "Less than requires exactly two parameters.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 2;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, 2 * sizeof(sql_data_type_t));
    update->expected_data_types[0] = f->parameters[0]->data_type;
    update->expected_data_types[1] = f->parameters[0]->data_type;  // both parameters should have the same data type
    update->return_type = SQL_TYPE_BOOL;

    if(f->parameters[0]->data_type != f->parameters[1]->data_type) {
        if(f->parameters[0]->data_type == SQL_TYPE_INT && f->parameters[1]->data_type == SQL_TYPE_DOUBLE) {
            update->expected_data_types[0] = SQL_TYPE_DOUBLE;  // if one is int and the other is double, the int should be converted to double
            update->expected_data_types[1] = SQL_TYPE_DOUBLE;
        }
    }

    sql_data_type_t data_type = f->parameters[0]->data_type;
    if(data_type == SQL_TYPE_BOOL) {
        update->implementation = sql_bool_less;
    } else if(data_type == SQL_TYPE_INT) {
        update->implementation = sql_int_less;
    } else if(data_type == SQL_TYPE_DOUBLE) {
        update->implementation = sql_double_less;
    } else if(data_type == SQL_TYPE_STRING) {
        update->implementation = sql_string_less;
    } else if(data_type == SQL_TYPE_DATETIME) {
        update->implementation = sql_datetime_less;
    } else {
        sql_ctx_error(ctx, "Less than is not supported for data type %s.", sql_data_type_name(data_type));
        return NULL;
    }
    return update;
}

sql_ctx_spec_t less_spec = {
    .name = "<",
    .description = "Less than",
    .update = update_less_spec
};

static sql_ctx_spec_update_t *update_less_or_equal_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 2) {
        sql_ctx_error(ctx, "Less than or equal requires exactly two parameters.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 2;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, 2 * sizeof(sql_data_type_t));
    update->expected_data_types[0] = f->parameters[0]->data_type;
    update->expected_data_types[1] = f->parameters[0]->data_type;  // both parameters should have the same data type
    update->return_type = SQL_TYPE_BOOL;

    if(f->parameters[0]->data_type != f->parameters[1]->data_type) {
        if(f->parameters[0]->data_type == SQL_TYPE_INT && f->parameters[1]->data_type == SQL_TYPE_DOUBLE) {
            update->expected_data_types[0] = SQL_TYPE_DOUBLE;  // if one is int and the other is double, the int should be converted to double
            update->expected_data_types[1] = SQL_TYPE_DOUBLE;
        }
    }

    sql_data_type_t data_type = f->parameters[0]->data_type;
    if(data_type == SQL_TYPE_BOOL) {
        update->implementation = sql_bool_less_or_equal;
    } else if(data_type == SQL_TYPE_INT) {
        update->implementation = sql_int_less_or_equal;
    } else if(data_type == SQL_TYPE_DOUBLE) {
        update->implementation = sql_double_less_or_equal;
    } else if(data_type == SQL_TYPE_STRING) {
        update->implementation = sql_string_less_or_equal;
    } else if(data_type == SQL_TYPE_DATETIME) {
        update->implementation = sql_datetime_less_or_equal;
    } else {
        sql_ctx_error(ctx, "Less than or equal is not supported for data type %s.", sql_data_type_name(data_type));
        return NULL;
    }
    return update;
}

sql_ctx_spec_t less_or_equal_spec = {
    .name = "<=",
    .description = "Less than or equal",
    .update = update_less_or_equal_spec
};

static sql_ctx_spec_update_t *update_not_equal_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 2) {
        sql_ctx_error(ctx, "Not equal requires exactly two parameters.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 2;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, 2 * sizeof(sql_data_type_t));
    update->expected_data_types[0] = f->parameters[0]->data_type;
    update->expected_data_types[1] = f->parameters[0]->data_type;  // both parameters should have the same data type
    update->return_type = SQL_TYPE_BOOL;

    if(f->parameters[0]->data_type != f->parameters[1]->data_type) {
        if(f->parameters[0]->data_type == SQL_TYPE_INT && f->parameters[1]->data_type == SQL_TYPE_DOUBLE) {
            update->expected_data_types[0] = SQL_TYPE_DOUBLE;  // if one is int and the other is double, the int should be converted to double
            update->expected_data_types[1] = SQL_TYPE_DOUBLE;
        }
    }

    sql_data_type_t data_type = f->parameters[0]->data_type;
    if(data_type == SQL_TYPE_BOOL) {
        update->implementation = sql_bool_not_equal;
    } else if(data_type == SQL_TYPE_INT) {
        update->implementation = sql_int_not_equal;
    } else if(data_type == SQL_TYPE_DOUBLE) {
        update->implementation = sql_double_not_equal;
    } else if(data_type == SQL_TYPE_STRING) {
        update->implementation = sql_string_not_equal;
    } else if(data_type == SQL_TYPE_DATETIME) {
        update->implementation = sql_datetime_not_equal;
    } else {
        sql_ctx_error(ctx, "Not equal is not supported for data type %s.", sql_data_type_name(data_type));
        return NULL;
    }
    return update;
}

sql_ctx_spec_t not_equal_spec = {
    .name = "!=",
    .description = "Not equal",
    .update = update_not_equal_spec
};

static sql_ctx_spec_update_t *update_equal_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 2) {
        sql_ctx_error(ctx, "Equal requires exactly two parameters.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 2;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, 2 * sizeof(sql_data_type_t));
    update->expected_data_types[0] = f->parameters[0]->data_type;
    update->expected_data_types[1] = f->parameters[0]->data_type;  // both parameters should have the same data type
    update->return_type = SQL_TYPE_BOOL;

    if(f->parameters[0]->data_type != f->parameters[1]->data_type) {
        if(f->parameters[0]->data_type == SQL_TYPE_INT && f->parameters[1]->data_type == SQL_TYPE_DOUBLE) {
            update->expected_data_types[0] = SQL_TYPE_DOUBLE;  // if one is int and the other is double, the int should be converted to double
            update->expected_data_types[1] = SQL_TYPE_DOUBLE;
        }
    }

    sql_data_type_t data_type = f->parameters[0]->data_type;
    if(data_type == SQL_TYPE_BOOL) {
        update->implementation = sql_bool_equal;
    } else if(data_type == SQL_TYPE_INT) {
        update->implementation = sql_int_equal;
    } else if(data_type == SQL_TYPE_DOUBLE) {
        update->implementation = sql_double_equal;
    } else if(data_type == SQL_TYPE_STRING) {
        update->implementation = sql_string_equal;
    } else if(data_type == SQL_TYPE_DATETIME) {
        update->implementation = sql_datetime_equal;
    } else {
        sql_ctx_error(ctx, "Equal is not supported for data type %s.", sql_data_type_name(data_type));
        return NULL;
    }
    return update;
}

sql_ctx_spec_t equal_spec = {
    .name = "=",
    .description = "Equal",
    .update = update_equal_spec
};

sql_ctx_spec_t double_equal_spec = {
    .name = "==",
    .description = "Equal",
    .update = update_equal_spec
};


void sql_register_comparison(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &less_spec);
    sql_ctx_register_spec(ctx, &less_or_equal_spec);
    sql_ctx_register_spec(ctx, &not_equal_spec);
    sql_ctx_register_spec(ctx, &equal_spec);
    sql_ctx_register_spec(ctx, &double_equal_spec);

    sql_ctx_register_callback(ctx, sql_bool_less, "bool_less", "Compare two boolean values");
    sql_ctx_register_callback(ctx, sql_bool_less_or_equal, "bool_less_or_equal", "Compare two boolean values");
    sql_ctx_register_callback(ctx, sql_bool_not_equal, "bool_not_equal", "Compare two boolean values");
    sql_ctx_register_callback(ctx, sql_bool_equal, "bool_equal", "Compare two boolean values");
    sql_ctx_register_callback(ctx, sql_int_less, "int_less", "Compare two integer values");
    sql_ctx_register_callback(ctx, sql_int_less_or_equal, "int_less_or_equal", "Compare two integer values");
    sql_ctx_register_callback(ctx, sql_int_not_equal, "int_not_equal", "Compare two integer values");
    sql_ctx_register_callback(ctx, sql_int_equal, "int_equal", "Compare two integer values");
    sql_ctx_register_callback(ctx, sql_double_less, "double_less", "Compare two double values");
    sql_ctx_register_callback(ctx, sql_double_less_or_equal, "double_less_or_equal", "Compare two double values");
    sql_ctx_register_callback(ctx, sql_double_not_equal, "double_not_equal", "Compare two double values");
    sql_ctx_register_callback(ctx, sql_double_equal, "double_equal", "Compare two double values");
    sql_ctx_register_callback(ctx, sql_string_less, "string_less", "Compare two string values");
    sql_ctx_register_callback(ctx, sql_string_less_or_equal, "string_less_or_equal", "Compare two string values");
    sql_ctx_register_callback(ctx, sql_string_not_equal, "string_not_equal", "Compare two string values");
    sql_ctx_register_callback(ctx, sql_string_equal, "string_equal", "Compare two string values");
    sql_ctx_register_callback(ctx, sql_datetime_less, "datetime_less", "Compare two datetime values");
    sql_ctx_register_callback(ctx, sql_datetime_less_or_equal, "datetime_less_or_equal", "Compare two datetime values");
    sql_ctx_register_callback(ctx, sql_datetime_not_equal, "datetime_not_equal", "Compare two datetime values");
    sql_ctx_register_callback(ctx, sql_datetime_equal, "datetime_equal", "Compare two datetime values");
}
