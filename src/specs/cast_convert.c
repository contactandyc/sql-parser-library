// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/date_utils.h"
#include <strings.h>


static sql_node_t *sql_convert_bool_to_int(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_int_init(ctx, 0, true);
    }
    return sql_int_init(ctx, child->value.bool_value ? 1 : 0, false);
}

static sql_node_t *sql_convert_bool_to_double(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_double_init(ctx, 0.0f, true);
    }
    return sql_double_init(ctx, child->value.bool_value ? 1.0f : 0.0f, false);
}

static sql_node_t *sql_convert_bool_to_string(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_string_init(ctx, NULL, true);
    }
    return sql_string_init(ctx, child->value.bool_value ? "true" : "false", false);
}

static sql_node_t *sql_convert_int_to_bool(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, child->value.int_value != 0, false);
}

static sql_node_t *sql_convert_int_to_datetime(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_datetime_init(ctx, 0, true); // Return NULL
    }

    // Convert the integer to DATETIME (i.e., Unix timestamp to time_t)
    time_t epoch = (time_t)child->value.int_value;

    // Return the converted datetime
    return sql_datetime_init(ctx, epoch, false);
}

static sql_node_t *sql_convert_int_to_double(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_double_init(ctx, 0.0f, true);
    }
    return sql_double_init(ctx, (double)child->value.int_value, false);
}

static sql_node_t *sql_convert_int_to_string(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_string_init(ctx, NULL, true);
    }
    char *result = (char *)aml_pool_strdupf(ctx->pool, "%d", child->value.int_value);
    return sql_string_init(ctx, result, false);
}

static sql_node_t *sql_convert_double_to_bool(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    return sql_bool_init(ctx, child->value.double_value != 0.0f, false);
}

static sql_node_t *sql_convert_double_to_datetime(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_datetime_init(ctx, 0, true); // Return NULL if the value is NULL
    }

    // Get the integer and fractional part of the double
    double double_value = child->value.double_value;
    time_t seconds = (time_t)double_value;          // Integer part represents seconds

    // Return the datetime value (you can use `tv.tv_sec` to store seconds, and `tv.tv_usec` for microseconds)
    return sql_datetime_init(ctx, seconds, false); // Assuming we store seconds as epoch time
}

static sql_node_t *sql_convert_double_to_int(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_int_init(ctx, 0, true);
    }
    return sql_int_init(ctx, (int)child->value.double_value, false);
}

static sql_node_t *sql_convert_double_to_string(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_string_init(ctx, NULL, true);
    }
    char *result = (char *)aml_pool_strdupf(ctx->pool, "%f", child->value.double_value);
    return sql_string_init(ctx, result, false);
}

static sql_node_t *sql_convert_string_to_bool(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_bool_init(ctx, false, true);
    }
    // convert True, 1 to true, False, 0 to false
    bool result = child->value.string_value && (strcasecmp(child->value.string_value, "TRUE") == 0 || strcmp(child->value.string_value, "1") == 0);
    if (!result)
        // convert FALSE, false, 0 to false
        result = child->value.string_value && (strcasecmp(child->value.string_value, "FALSE") == 0 || strcmp(child->value.string_value, "0") == 0);
        if (!result)
            return sql_bool_init(ctx, false, true);
    return sql_bool_init(ctx, result, false);
}

static sql_node_t *sql_convert_string_to_int(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_int_init(ctx, 0, true);
    }
    int result;
    if (sscanf(child->value.string_value, "%d", &result) != 1) {
        return sql_int_init(ctx, 0, true);
    }
    return sql_int_init(ctx, result, false);
}

static sql_node_t *sql_convert_string_to_double(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_double_init(ctx, 0.0f, true);
    }
    double result;
    if (sscanf(child->value.string_value, "%lf", &result) != 1) {
        return sql_double_init(ctx, 0.0f, true);
    }
    return sql_double_init(ctx, result, false);
}

static sql_node_t *sql_convert_string_to_datetime(sql_ctx_t *ctx, sql_node_t *f) {
    // Evaluate the parameter
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_datetime_init(ctx, 0, true); // Return NULL
    }

    const char *date_str = child->value.string_value;
    if (!date_str || strlen(date_str) == 0) {
        return sql_datetime_init(ctx, 0, true); // Return NULL for empty string
    }

    if (!strncasecmp(date_str, "INTERVAL", 8) && child->type == SQL_COMPOUND_LITERAL) {
        return sql_compound_init(ctx, date_str, false); // Return the interval string as is
    }
    time_t epoch = 0;
    if (!date_utils_convert_string_to_datetime(&epoch, ctx->pool, date_str)) {
        sql_ctx_error(ctx, "Failed to convert string to datetime: %s\n", date_str);
        return sql_datetime_init(ctx, 0, true); // Return NULL
    }
    return sql_datetime_init(ctx, epoch, false); // Return the datetime value
}

static sql_node_t *sql_convert_datetime_to_string(sql_ctx_t *ctx, sql_node_t *f) {
    // Evaluate the parameter
    sql_node_t *child = sql_eval(ctx, f->parameters[0]);
    if (!child || child->is_null) {
        return sql_string_init(ctx, NULL, true); // Return NULL
    }

    // Check that the parameter is of type DATETIME
    if (child->data_type != SQL_TYPE_DATETIME) {
        fprintf(stderr, "Invalid type for CONVERT_DATETIME_TO_STRING: parameter must be DATETIME\n");
        return sql_string_init(ctx, NULL, true);
    }

    char *result = date_utils_convert_epoch_to_iso_utc(ctx->pool, child->value.epoch);
    if(!result) {
        fprintf(stderr, "Failed to format datetime\n");
        return sql_string_init(ctx, NULL, true);
    }
    return sql_string_init(ctx, result, false);
}

static sql_node_t *sql_convert_value(sql_ctx_t *ctx, sql_node_t *value, sql_data_type_t target_type) {
    if (!value || value->is_null) {
        return sql_bool_init(ctx, false, true);
    }

    // If already the correct type, return as is
    if (value->data_type == target_type) {
        return value;
    }

    // Create a function node for the conversion
    sql_node_t *convert_node = sql_function_init(ctx, "CONVERT");
    convert_node->num_parameters = 1;
    convert_node->parameters = (sql_node_t **)aml_pool_alloc(ctx->pool, sizeof(sql_node_t *));
    convert_node->parameters[0] = value;
    convert_node->data_type = target_type;
    convert_node->is_null = false;

    // Assign the correct function based on source and target types
    switch (value->data_type) {
        case SQL_TYPE_BOOL:
            if (target_type == SQL_TYPE_INT)
                convert_node->func = sql_convert_bool_to_int;
            else if (target_type == SQL_TYPE_DOUBLE)
                convert_node->func = sql_convert_bool_to_double;
            else if (target_type == SQL_TYPE_STRING)
                convert_node->func = sql_convert_bool_to_string;
            break;

        case SQL_TYPE_INT:
            if (target_type == SQL_TYPE_BOOL)
                convert_node->func = sql_convert_int_to_bool;
            else if (target_type == SQL_TYPE_DATETIME)
                convert_node->func = sql_convert_int_to_datetime;
            else if (target_type == SQL_TYPE_DOUBLE)
                convert_node->func = sql_convert_int_to_double;
            else if (target_type == SQL_TYPE_STRING)
                convert_node->func = sql_convert_int_to_string;
            break;

        case SQL_TYPE_DOUBLE:
            if (target_type == SQL_TYPE_BOOL)
                convert_node->func = sql_convert_double_to_bool;
            else if (target_type == SQL_TYPE_DATETIME)
                convert_node->func = sql_convert_double_to_datetime;
            else if (target_type == SQL_TYPE_INT)
                convert_node->func = sql_convert_double_to_int;
            else if (target_type == SQL_TYPE_STRING)
                convert_node->func = sql_convert_double_to_string;
            break;

        case SQL_TYPE_STRING:
            if (target_type == SQL_TYPE_BOOL)
                convert_node->func = sql_convert_string_to_bool;
            else if (target_type == SQL_TYPE_INT)
                convert_node->func = sql_convert_string_to_int;
            else if (target_type == SQL_TYPE_DOUBLE)
                convert_node->func = sql_convert_string_to_double;
            else if (target_type == SQL_TYPE_DATETIME)
                convert_node->func = sql_convert_string_to_datetime;
            break;

        case SQL_TYPE_DATETIME:
            if (target_type == SQL_TYPE_STRING)
                convert_node->func = sql_convert_datetime_to_string;
            break;

        default:
            sql_ctx_error(ctx, "Unsupported conversion from %s to %s.",
                          sql_data_type_name(value->data_type), sql_data_type_name(target_type));
            return sql_bool_init(ctx, false, true);
    }

    return convert_node;
}

static sql_node_t *sql_convert_list_to_type(sql_ctx_t *ctx, sql_node_t *f) {
    sql_node_t *list = sql_eval(ctx, f->parameters[0]);
    if (!list || list->is_null || list->type != SQL_LIST) {
        return sql_bool_init(ctx, false, true);
    }

    sql_data_type_t target_type = f->data_type;
    sql_node_t *converted_list = sql_list_init(ctx, list->num_parameters, false);

    for (size_t i = 0; i < list->num_parameters; i++) {
        converted_list->parameters[i] = sql_convert_value(ctx, list->parameters[i], target_type);
    }

    return converted_list;
}

static sql_data_type_t parse_data_type_from_string(const char *type_str) {
    if (strcasecmp(type_str, "INT") == 0 || strcasecmp(type_str, "INTEGER") == 0) {
        return SQL_TYPE_INT;
    } else if (strcasecmp(type_str, "DOUBLE") == 0 || strcasecmp(type_str, "DECIMAL") == 0 || strcasecmp(type_str, "NUMERIC") == 0) {
        return SQL_TYPE_DOUBLE;
    } else if (strcasecmp(type_str, "STRING") == 0 || strcasecmp(type_str, "VARCHAR") == 0 || strcasecmp(type_str, "CHAR") == 0) {
        return SQL_TYPE_STRING;
    } else if (strcasecmp(type_str, "DATETIME") == 0) {
        return SQL_TYPE_DATETIME;
    } else if (strcasecmp(type_str, "BOOL") == 0 || strcasecmp(type_str, "BOOLEAN") == 0) {
        return SQL_TYPE_BOOL;
    }
    return SQL_TYPE_UNKNOWN;
}

// Update function for CONVERT
static sql_ctx_spec_update_t *update_convert_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    sql_node_t **parameters = NULL;
    size_t num_parameters = 0;
    sql_data_type_t target_type = f->data_type;

    if(!strcasecmp(spec->name, "CONVERT")) {
        if (f->num_parameters != 2) {
            sql_ctx_error(ctx, "CONVERT function requires exactly two parameters.");
            return NULL;
        }
        target_type = parse_data_type_from_string(f->parameters[0]->token);
        if (target_type == SQL_TYPE_UNKNOWN) {
            sql_ctx_error(ctx, "Invalid data type for CONVERT: %s", f->parameters[0]->token);
            return NULL;
        }
        parameters = f->parameters+1;
        num_parameters = 1;
    } else if(!strcasecmp(spec->name, "CAST")) {
        if (f->num_parameters != 3 || strcasecmp(f->parameters[1]->token, "AS") != 0) {
            sql_ctx_error(ctx, "CAST function requires exactly three parameters with the second parameter being 'AS'.");
            return NULL;
        }
        target_type = parse_data_type_from_string(f->parameters[2]->token);
        if (target_type == SQL_TYPE_UNKNOWN) {
            sql_ctx_error(ctx, "Invalid data type for CAST: %s", f->parameters[2]->token);
            return NULL;
        }
        parameters = f->parameters;
        num_parameters = 1;
    } else if(!strcmp(spec->name, "::")) {
        if (f->num_parameters != 2) {
            sql_ctx_error(ctx, ":: function requires exactly two parameters.");
            return NULL;
        }
        target_type = parse_data_type_from_string(f->parameters[1]->token);
        if (target_type == SQL_TYPE_UNKNOWN) {
            sql_ctx_error(ctx, "Invalid data type for :: %s", f->parameters[1]->token);
            return NULL;
        }
        parameters = f->parameters;
        num_parameters = 1;
    } else {
        sql_ctx_error(ctx, "Unsupported function: %s", spec->name);
        return NULL;
    }

    sql_data_type_t input_type = parameters[0]->data_type;

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 1;
    update->parameters = parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, sizeof(sql_data_type_t));
    update->expected_data_types[0] = input_type;

    if (input_type == target_type) {
        return NULL;
    } else if(parameters[0]->type == SQL_LIST) {
        update->implementation = sql_convert_list_to_type;
        update->return_type = target_type;
        return update;
    } else if (input_type == SQL_TYPE_BOOL) {
        if (target_type == SQL_TYPE_INT) {
            update->implementation = sql_convert_bool_to_int;
        } else if (target_type == SQL_TYPE_DOUBLE) {
            update->implementation = sql_convert_bool_to_double;
        } else if (target_type == SQL_TYPE_STRING) {
            update->implementation = sql_convert_bool_to_string;
        }
    } else if (input_type == SQL_TYPE_INT) {
        if (target_type == SQL_TYPE_BOOL) {
            update->implementation = sql_convert_int_to_bool;
        } else if (target_type == SQL_TYPE_DATETIME) {
            update->implementation = sql_convert_int_to_datetime;
        } else if (target_type == SQL_TYPE_DOUBLE) {
            update->implementation = sql_convert_int_to_double;
        } else if (target_type == SQL_TYPE_STRING) {
            update->implementation = sql_convert_int_to_string;
        }
    } else if (input_type == SQL_TYPE_DOUBLE) {
        if (target_type == SQL_TYPE_BOOL) {
            update->implementation = sql_convert_double_to_bool;
        } else if (target_type == SQL_TYPE_DATETIME) {
            update->implementation = sql_convert_double_to_datetime;
        } else if (target_type == SQL_TYPE_INT) {
            update->implementation = sql_convert_double_to_int;
        } else if (target_type == SQL_TYPE_STRING) {
            update->implementation = sql_convert_double_to_string;
        }
    } else if (input_type == SQL_TYPE_STRING) {
        if (target_type == SQL_TYPE_BOOL) {
            update->implementation = sql_convert_string_to_bool;
        } else if (target_type == SQL_TYPE_INT) {
            update->implementation = sql_convert_string_to_int;
        } else if (target_type == SQL_TYPE_DOUBLE) {
            update->implementation = sql_convert_string_to_double;
        } else if (target_type == SQL_TYPE_DATETIME) {
            update->implementation = sql_convert_string_to_datetime;
        }
    } else if (input_type == SQL_TYPE_DATETIME) {
        if (target_type == SQL_TYPE_STRING) {
            update->implementation = sql_convert_datetime_to_string;
        }
    }

    if (update->implementation) {
        update->return_type = target_type;
    } else {
        sql_ctx_error(ctx, "Unsupported conversion from %s to %s.",
                      sql_data_type_name(input_type), sql_data_type_name(target_type));
        return NULL;
    }

    return update;
}

// Specification for CONVERT function
static sql_ctx_spec_t convert_spec = {
    .name = "CONVERT",
    .description = "Converts a value to a specified type.",
    .update = update_convert_spec
};

static sql_ctx_spec_t cast_spec = {
    .name = "CAST",
    .description = "Converts a value to a specified type.",
    .update = update_convert_spec
};

static sql_ctx_spec_t cast_operator_spec = {
    .name = "::",
    .description = "Converts a value to a specified type.",
    .update = update_convert_spec
};

// Register all CONVERT-related functions
void sql_register_convert(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &convert_spec);
    sql_ctx_register_spec(ctx, &cast_spec);
    sql_ctx_register_spec(ctx, &cast_operator_spec);

    // Register all individual conversion functions
    sql_ctx_register_callback(ctx, sql_convert_bool_to_int, "convert_bool_to_int", "Converts a BOOL to an INT.");
    sql_ctx_register_callback(ctx, sql_convert_bool_to_double, "convert_bool_to_double", "Converts a BOOL to a DOUBLE.");
    sql_ctx_register_callback(ctx, sql_convert_bool_to_string, "convert_bool_to_string", "Converts a BOOL to a STRING.");
    sql_ctx_register_callback(ctx, sql_convert_int_to_bool, "convert_int_to_bool", "Converts an INT to a BOOL.");
    sql_ctx_register_callback(ctx, sql_convert_int_to_datetime, "convert_int_to_datetime", "Converts an INT to a DATETIME.");
    sql_ctx_register_callback(ctx, sql_convert_int_to_double, "convert_int_to_double", "Converts an INT to a DOUBLE.");
    sql_ctx_register_callback(ctx, sql_convert_int_to_string, "convert_int_to_string", "Converts an INT to a STRING.");
    sql_ctx_register_callback(ctx, sql_convert_double_to_bool, "convert_double_to_bool", "Converts a DOUBLE to a BOOL.");
    sql_ctx_register_callback(ctx, sql_convert_double_to_int, "convert_double_to_int", "Converts a DOUBLE to an INT.");
    sql_ctx_register_callback(ctx, sql_convert_double_to_string, "convert_double_to_string", "Converts a DOUBLE to a STRING.");
    sql_ctx_register_callback(ctx, sql_convert_string_to_bool, "convert_string_to_bool", "Converts a STRING to a BOOL.");
    sql_ctx_register_callback(ctx, sql_convert_string_to_int, "convert_string_to_int", "Converts a STRING to an INT.");
    sql_ctx_register_callback(ctx, sql_convert_string_to_double, "convert_string_to_double", "Converts a STRING to a DOUBLE.");
    sql_ctx_register_callback(ctx, sql_convert_string_to_datetime, "date_utils_convert_string_to_datetime", "Converts a STRING to a DATETIME.");
    sql_ctx_register_callback(ctx, sql_convert_datetime_to_string, "convert_datetime_to_string", "Converts a DATETIME to a STRING.");
}
