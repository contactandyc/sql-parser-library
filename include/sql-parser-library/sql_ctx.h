// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef _sql_context_H
#define _sql_context_H

#include "the-macro-library/macro_map.h"
#include "sql-parser-library/sql_named_pointer.h"
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

/**
 * Callback used by the Binder to dynamically resolve schema requests.
 * Given the current execution context, a target column name, and an optional
 * table alias (which will be NULL when resolving standalone expressions),
 * this function returns a populated column descriptor, or NULL if the column cannot be found.
 */
typedef sql_ctx_column_t* (*sql_schema_lookup_cb)(
    sql_ctx_t *ctx,
    const char *table_name,
    const char *column_name
);

// --- ERROR HANDLING API ---
void sql_ctx_error(sql_ctx_t *ctx, const char *format, ...);
void sql_ctx_warning(sql_ctx_t *ctx, const char *format, ...);
char **sql_ctx_get_errors(sql_ctx_t *ctx, size_t *num_errors);
char **sql_ctx_get_warnings(sql_ctx_t *ctx, size_t *num_warnings);
void sql_ctx_print_messages(sql_ctx_t *ctx);
void sql_ctx_clear_messages(sql_ctx_t *ctx);

// --- REGISTRATION API ---
void sql_ctx_register_callback(sql_ctx_t *ctx, void *callback, const char *name, const char *description);
void *sql_ctx_get_callback(sql_ctx_t *ctx, const char *name);
const char *sql_ctx_get_callback_name(sql_ctx_t *ctx, void *callback);
const char *sql_ctx_get_callback_description(sql_ctx_t *ctx, void *callback);

void sql_ctx_reserve_keyword(sql_ctx_t *ctx, const char *keyword);
bool sql_ctx_is_reserved_keyword(sql_ctx_t *ctx, const char *keyword);

void sql_ctx_register_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec);
sql_ctx_spec_t *sql_ctx_get_spec(sql_ctx_t *ctx, const char *name);

// Add this small struct right before struct sql_ctx_s
typedef struct sql_ctx_pool_node_s {
    aml_pool_t *pool;
    struct sql_ctx_pool_node_s *next;
} sql_ctx_pool_node_t;

/**
 * The Global Execution Context.
 * Acts as the central memory arena and state container for parsing, compiling, and execution.
 */
struct sql_ctx_s {
    aml_pool_t *pool;              // The global arena allocator for this query

    sql_schema_lookup_cb schema_lookup; // Dynamic catalog resolver
    void *catalog_state;           // Internal routing state for the VM

    struct sql_vm_s *vm;           // Active VM instance (allows subquery compilation)
    struct sql_ctx_s *parent_ctx;  // Pointer to the outer query's context

    sql_ctx_pool_node_t *tracked_pools;

    int time_zone_offset;          // Global timezone delta

    sql_ctx_message_t *errors;     // Linked list of parse/compile errors
    sql_ctx_message_t *warnings;   // Linked list of warnings

    macro_map_t *reserved_keywords;// Fast-lookup tree for SQL syntax
    sql_named_pointer_t callbacks; // Registry of physical C functions
    macro_map_t *specs;            // Registry of SQL function specifications

    void *row;                     // DYNAMIC STATE: The current tuple array being evaluated
    void **current_agg_states;     // DYNAMIC STATE: The active aggregate counters for the current GROUP
};

/** Defines a physical schema column bound to an execution callback. */
struct sql_ctx_column_s {
    char *name;
    sql_data_type_t type;
    sql_node_cb func;        // The C-accessor to physically fetch the data
    char *table_name;
    int table_index;         // The execution array index inside the VM
    int scope_depth;         // How many parent scopes to traverse to find the data
    void *custom_data;
};

/** Defines type-casting and optimization rules for functions during compilation. */
struct sql_ctx_spec_update_s {
    sql_data_type_t *expected_data_types;
    sql_node_t **parameters;
    size_t num_parameters;
    sql_data_type_t return_type;
    sql_node_cb implementation;
};

typedef sql_ctx_spec_update_t *(*sql_ctx_update_cb)(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f);

/**
 * Defines the signature, rules, and lifecycle of a registered SQL function.
 */
struct sql_ctx_spec_s {
    const char *name;           // e.g., "SUM"
    const char *description;
    sql_ctx_update_cb update;   // Optimization and Type-Checking callback

    // --- AGGREGATE LIFECYCLE ---
    bool is_aggregate;
    size_t state_size;          // Bytes required to store intermediate state
    void (*agg_init)(void *state);
    void (*agg_step)(sql_ctx_t *ctx, struct sql_node_s *f, void *state);
    struct sql_node_s *(*agg_finalize)(sql_ctx_t *ctx, struct sql_node_s *f, void *state);

    bool is_volatile;           // True if result changes per-row (e.g. RANDOM(), NOW())
    bool is_window_func;        // True if this requires an OVER clause
};

// --- STANDARD LIBRARY REGISTRATIONS ---
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
bool sql_is_valid_extract(const char *value);

void sql_register_avg(sql_ctx_t *ctx);
void sql_register_count(sql_ctx_t *ctx);
void sql_register_sum(sql_ctx_t *ctx);

void sql_register_row_number(sql_ctx_t *ctx);
void sql_register_rank(sql_ctx_t *ctx);

/** Registers all built-in keywords, operators, and functions into the Context. */
static inline void sql_register_ctx(sql_ctx_t *ctx) {
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
    sql_register_avg(ctx);
    sql_register_count(ctx);
    sql_register_sum(ctx);
    sql_register_row_number(ctx);
    sql_register_rank(ctx);
}

// --- EVALUATION ENGINE & COMPILER UTILITIES ---

/**
 * The core execution loop.
 * Recursively evaluates a compiled node against the current row in the Virtual Machine.
 */
sql_node_t *sql_eval(sql_ctx_t *ctx, sql_node_t *f);

// --- NODE CONSTRUCTORS ---
// These functions allocate and initialize strongly-typed execution nodes.
// They are heavily used by the compiler to replace AST literals with memory-safe VM structures,
// and by C-callbacks to return computed results back to the Virtual Machine.
sql_node_t *sql_bool_init(sql_ctx_t *ctx, bool value, bool is_null);
sql_node_t *sql_list_init(sql_ctx_t *ctx, size_t num_elements, bool is_null);
sql_node_t *sql_int_init(sql_ctx_t *ctx, int value, bool is_null);
sql_node_t *sql_double_init(sql_ctx_t *ctx, double value, bool is_null);
sql_node_t *sql_string_init(sql_ctx_t *ctx, const char *value, bool is_null);
sql_node_t *sql_compound_init(sql_ctx_t *ctx, const char *value, bool is_null);
sql_node_t *sql_datetime_init(sql_ctx_t *ctx, time_t epoch, bool is_null);
sql_node_t *sql_function_init(sql_ctx_t *ctx, const char *name);

// --- COMPILER & OPTIMIZER PASSES ---
// These functions are executed exactly once during the query planning phase.

/** Translates the raw, string-based AST into strongly-typed execution nodes. */
sql_node_t *sql_convert_ast_to_node(sql_ctx_t *context, struct sql_ast_node_s *ast);

/** Injects CAST operations where implicit type coercion is required (e.g., INT + DOUBLE). */
void sql_apply_type_conversions(sql_ctx_t *context, sql_node_t *node);

/** Constant Folding: Pre-evaluates static math and logical expressions to speed up the VM. */
void sql_simplify_tree(sql_ctx_t *ctx, sql_node_t *node);
void sql_simplify_func_tree(sql_ctx_t *context, sql_node_t *node );
void sql_simplify_logical_expressions(sql_node_t *node);

// --- TYPE MANAGEMENT & UTILITIES ---

/** Determines the safest common type for binary operations (e.g., INT and DOUBLE yields DOUBLE). */
sql_data_type_t sql_determine_common_type(sql_data_type_t type1, sql_data_type_t type2);

/** Wraps a node in a CONVERT function to force it to a target type at runtime. */
sql_node_t *sql_convert(sql_ctx_t *context, sql_node_t *param, sql_data_type_t target_type);

/** Performs a deep clone of an execution tree. Vital for storing rows in Hash/Group buckets. */
sql_node_t *sql_copy_nodes(sql_ctx_t *ctx, sql_node_t *node);

/** Pretty-prints the compiled execution tree for debugging. */
void sql_print_node(sql_ctx_t *ctx, sql_node_t *node, int depth);

/** Allocates a new memory pool and tracks it for automatic garbage collection */
aml_pool_t *sql_ctx_allocate_tracked_pool(sql_ctx_t *ctx, size_t size);

/** Destroys all tracked memory pools attached to the context */
void sql_ctx_destroy_tracked_pools(sql_ctx_t *ctx);

#endif
