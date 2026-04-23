// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef _sql_context_H
#define _sql_context_H

#include "the-macro-library/macro_map.h"
#include "sql-parser-library/named_pointer.h"
#include "sql-parser-library/sql_node.h"

struct sql_ctx_s;
typedef struct sql_ctx_s sql_ctx_t;

struct sql_ctx_column_s;
typedef struct sql_ctx_column_s sql_ctx_column_t;

struct sql_ast_node_s;

struct sql_ctx_spec_s;
typedef struct sql_ctx_spec_s sql_ctx_spec_t;

struct sql_ctx_spec_update_s;
typedef struct sql_ctx_spec_update_s sql_ctx_spec_update_t;

struct sql_ctx_message_s;
typedef struct sql_ctx_message_s sql_ctx_message_t;

// --- DYNAMIC CATALOG CALLBACK ---
typedef sql_ctx_column_t* (*sql_schema_lookup_cb)(
    sql_ctx_t *ctx,
    const char *table_name,
    const char *column_name
);

void sql_ctx_error(sql_ctx_t *ctx, const char *format, ...);
void sql_ctx_warning(sql_ctx_t *ctx, const char *format, ...);
char **sql_ctx_get_errors(sql_ctx_t *ctx, size_t *num_errors);
char **sql_ctx_get_warnings(sql_ctx_t *ctx, size_t *num_warnings);
void sql_ctx_print_messages(sql_ctx_t *ctx);
void sql_ctx_clear_messages(sql_ctx_t *ctx);

void sql_ctx_register_callback(sql_ctx_t *ctx, void *callback, const char *name, const char *description);
void *sql_ctx_get_callback(sql_ctx_t *ctx, const char *name);
const char *sql_ctx_get_callback_name(sql_ctx_t *ctx, void *callback);
const char *sql_ctx_get_callback_description(sql_ctx_t *ctx, void *callback);

void sql_ctx_reserve_keyword(sql_ctx_t *ctx, const char *keyword);
bool sql_ctx_is_reserved_keyword(sql_ctx_t *ctx, const char *keyword);

void sql_ctx_register_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec);
sql_ctx_spec_t *sql_ctx_get_spec(sql_ctx_t *ctx, const char *name);

struct sql_ctx_s {
    aml_pool_t *pool;

    sql_schema_lookup_cb schema_lookup;
    void *catalog_state;

    int time_zone_offset;

    sql_ctx_message_t *errors;
    sql_ctx_message_t *warnings;
    macro_map_t *reserved_keywords;
    named_pointer_t callbacks;
    macro_map_t *specs;

    void *row;

    // --- NEW: The active aggregate states for the current group! ---
    void **current_agg_states;
};

struct sql_ctx_column_s {
    char *name;
    sql_data_type_t type;
    sql_node_cb func;
    char *table_name;
    int table_index;
};

struct sql_ctx_spec_update_s {
    sql_data_type_t *expected_data_types;
    sql_node_t **parameters;
    size_t num_parameters;
    sql_data_type_t return_type;
    sql_node_cb implementation;
};

typedef sql_ctx_spec_update_t *(*sql_ctx_update_cb)(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f);

struct sql_ctx_spec_s {
    const char *name;
    const char *description;
    sql_ctx_update_cb update;

    // --- NEW: AGGREGATE LIFECYCLE ---
    bool is_aggregate;
    size_t state_size;
    void (*agg_init)(void *state);
    void (*agg_step)(sql_ctx_t *ctx, struct sql_node_s *f, void *state);
    struct sql_node_s *(*agg_finalize)(sql_ctx_t *ctx, struct sql_node_s *f, void *state);
};

void sql_reserve_default_keywords(sql_ctx_t *ctx);
void sql_register_arithmetic(sql_ctx_t *ctx);
void sql_register_boolean(sql_ctx_t *ctx);
void sql_register_between(sql_ctx_t *ctx);
void sql_register_comparison(sql_ctx_t *ctx);
void sql_register_is_boolean(sql_ctx_t *ctx);
void sql_register_is_null(sql_ctx_t *ctx);
void sql_register_in(sql_ctx_t *ctx);
void sql_register_like(sql_ctx_t *ctx);
void sql_register_convert(sql_ctx_t *ctx);
void sql_register_coalesce(sql_ctx_t *ctx);
void sql_register_concat(sql_ctx_t *ctx);
void sql_register_convert_tz(sql_ctx_t *ctx);
void sql_register_date_trunc(sql_ctx_t *ctx);
void sql_register_extract(sql_ctx_t *ctx);
void sql_register_length(sql_ctx_t *ctx);
void sql_register_lower_upper(sql_ctx_t *ctx);
void sql_register_min_max(sql_ctx_t *ctx);
void sql_register_now(sql_ctx_t *ctx);
void sql_register_round(sql_ctx_t *ctx);
void sql_register_substr(sql_ctx_t *ctx);
void sql_register_trim(sql_ctx_t *ctx);
void sql_register_geo(sql_ctx_t *ctx);
void sql_register_nullif(sql_ctx_t *ctx);
void sql_register_string_ext(sql_ctx_t *ctx);
void sql_register_arithmetic_ext(sql_ctx_t *ctx);
void sql_register_case(sql_ctx_t *ctx);
void sql_register_regex(sql_ctx_t *ctx);
void sql_register_json(sql_ctx_t *ctx);
bool is_valid_extract(const char *value);

// Group Functions
void sql_register_avg(sql_ctx_t *ctx);
void sql_register_count(sql_ctx_t *ctx);
void sql_register_sum(sql_ctx_t *ctx);


static inline
void register_ctx(sql_ctx_t *ctx) {
    sql_reserve_default_keywords(ctx);
    sql_register_arithmetic(ctx);
    sql_register_boolean(ctx);
    sql_register_between(ctx);
    sql_register_coalesce(ctx);
    sql_register_comparison(ctx);
    sql_register_convert_tz(ctx);
    sql_register_concat(ctx);
    sql_register_date_trunc(ctx);
    sql_register_extract(ctx);
    sql_register_is_boolean(ctx);
    sql_register_is_null(ctx);
    sql_register_in(ctx);
    sql_register_like(ctx);
    sql_register_convert(ctx);
    sql_register_length(ctx);
    sql_register_lower_upper(ctx);
    sql_register_min_max(ctx);
    sql_register_now(ctx);
    sql_register_round(ctx);
    sql_register_substr(ctx);
    sql_register_trim(ctx);
    sql_register_geo(ctx);
    sql_register_nullif(ctx);
    sql_register_string_ext(ctx);
    sql_register_arithmetic_ext(ctx);
    sql_register_case(ctx);
    sql_register_regex(ctx);
    sql_register_json(ctx);

    // Group Functions
    sql_register_avg(ctx);
    sql_register_count(ctx);
    sql_register_sum(ctx);
}

sql_node_t *sql_eval(sql_ctx_t *ctx, sql_node_t *f);
sql_node_t *sql_bool_init(sql_ctx_t *ctx, bool value, bool is_null);
sql_node_t *sql_list_init(sql_ctx_t *ctx, size_t num_elements, bool is_null);
sql_node_t *sql_int_init(sql_ctx_t *ctx, int value, bool is_null);
sql_node_t *sql_double_init(sql_ctx_t *ctx, double value, bool is_null);
sql_node_t *sql_string_init(sql_ctx_t *ctx, const char *value, bool is_null);
sql_node_t *sql_compound_init(sql_ctx_t *ctx, const char *value, bool is_null);
sql_node_t *sql_datetime_init(sql_ctx_t *ctx, time_t epoch, bool is_null);
sql_node_t *sql_function_init(sql_ctx_t *ctx, const char *name);
void apply_type_conversions(sql_ctx_t *context, sql_node_t *node);
void simplify_tree(sql_ctx_t *ctx, sql_node_t *node);
sql_node_t *copy_nodes(sql_ctx_t *ctx, sql_node_t *node);
void simplify_func_tree(sql_ctx_t *context, sql_node_t *node );
void simplify_logical_expressions(sql_node_t *node);
sql_data_type_t sql_determine_common_type(sql_data_type_t type1, sql_data_type_t type2);
sql_node_t *sql_convert(sql_ctx_t *context, sql_node_t *param, sql_data_type_t target_type);
sql_node_t *convert_ast_to_node(sql_ctx_t *context, struct sql_ast_node_s *ast);
void print_node(sql_ctx_t *ctx, sql_node_t *node, int depth);

#endif
