// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"
#include "the-macro-library/macro_map.h"
#include <string.h>
#include <strings.h>

typedef struct {
    macro_map_t node;
    char *name;
} sql_ctx_name_t;

static inline int sql_ctx_name_insert_compare(const sql_ctx_name_t *a,
                                              const sql_ctx_name_t *b) {
    return strcasecmp(a->name, b->name);
}

static inline int sql_ctx_name_find_compare(const char *name, const sql_ctx_name_t *o) {
    return strcasecmp(name, o->name);
}

static inline
macro_map_insert(sql_ctx_name_insert, sql_ctx_name_t,
                 sql_ctx_name_insert_compare);

static inline
macro_map_find_kv(sql_ctx_name_find, char, sql_ctx_name_t,
                  sql_ctx_name_find_compare);

typedef struct {
    macro_map_t node;
    sql_ctx_spec_t *spec;
} sql_ctx_spec_node_t;

static inline int sql_ctx_spec_insert_compare(const sql_ctx_spec_node_t *a,
                                              const sql_ctx_spec_node_t *b) {
    return strcasecmp(a->spec->name, b->spec->name);
}

static inline int sql_ctx_spec_find_compare(const char *name, const sql_ctx_spec_node_t *o) {
    return strcasecmp(name, o->spec->name);
}

static inline
macro_map_insert(sql_ctx_spec_insert, sql_ctx_spec_node_t,
                 sql_ctx_spec_insert_compare);

static inline
macro_map_find_kv(sql_ctx_spec_find, char, sql_ctx_spec_node_t,
                  sql_ctx_spec_find_compare);

struct sql_ctx_message_s {
    char *message;
    sql_ctx_message_t *next;
};

void sql_ctx_register_callback(sql_ctx_t *ctx, void *callback, const char *name, const char *description) {
    register_named_pointer(ctx->pool, &ctx->callbacks, callback, name, description);
}

const char *sql_ctx_get_callback_name(sql_ctx_t *ctx, void *callback) {
    return get_named_pointer_name(&ctx->callbacks, callback);
}

const char *sql_ctx_get_callback_description(sql_ctx_t *ctx, void *callback) {
    return get_named_pointer_description(&ctx->callbacks, callback);
}

void *sql_ctx_get_callback(sql_ctx_t *ctx, const char *name) {
    return get_named_pointer_pointer(&ctx->callbacks, name);
}

void sql_ctx_reserve_keyword(sql_ctx_t *ctx, const char *keyword) {
    if (!ctx || !keyword) return;

    sql_ctx_name_t *p = sql_ctx_name_find(ctx->reserved_keywords, keyword);
    if (p)
        return;

    p = (sql_ctx_name_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_name_t));
    p->name = aml_pool_strdup(ctx->pool, keyword);
    sql_ctx_name_insert(&ctx->reserved_keywords, p);
}

bool sql_ctx_is_reserved_keyword(sql_ctx_t *ctx, const char *keyword) {
    if (!ctx || !keyword) return false;

    sql_ctx_name_t *p = sql_ctx_name_find(ctx->reserved_keywords, keyword);
    return p != NULL;
}

void sql_reserve_default_keywords(sql_ctx_t *ctx) {
    static const char *sql_keywords[] = {
        "SELECT", "FROM", "WHERE", "ORDER", "BY", "ASC", "DESC",
        "JOIN", "ON", "INNER", "LEFT", "RIGHT", "FULL", "OUTER",
        "GROUP", "HAVING", "LIMIT", "OFFSET",
        "AS", "IS", "DISTINCT", "CASE", "WHEN", "THEN", "ELSE", "END", "EXISTS",
        "DOUBLE", "FLOAT", "INT", "INTEGER", "BOOL", "BOOLEAN", "DATETIME"
    };
    for(size_t i = 0; i < sizeof(sql_keywords) / sizeof(sql_keywords[0]); i++) {
        sql_ctx_reserve_keyword(ctx, sql_keywords[i]);
    }
}

void sql_ctx_error(sql_ctx_t *ctx, const char *format, ...) {
    if (!ctx || !format) return;

    sql_ctx_message_t *message = (sql_ctx_message_t *)aml_pool_alloc(ctx->pool, sizeof(sql_ctx_message_t));
    va_list args;
    va_start(args, format);
    message->message = aml_pool_strdupvf(ctx->pool, format, args);
    va_end(args);

    // Append to the errors list
    message->next = ctx->errors;
    ctx->errors = message;
}

void sql_ctx_warning(sql_ctx_t *ctx, const char *format, ...) {
    if (!ctx || !format) return;

    sql_ctx_message_t *message = (sql_ctx_message_t *)aml_pool_alloc(ctx->pool, sizeof(sql_ctx_message_t));
    va_list args;
    va_start(args, format);
    message->message = aml_pool_strdupvf(ctx->pool, format, args);
    va_end(args);

    // Append to the warnings list
    message->next = ctx->warnings;
    ctx->warnings = message;
}

char **sql_ctx_get_errors(sql_ctx_t *ctx, size_t *num_errors) {
    if (!ctx) return NULL;

    size_t count = 0;
    sql_ctx_message_t *error = ctx->errors;
    while (error) {
        count++;
        error = error->next;
    }

    if (count == 0) {
        *num_errors = 0;
        return NULL;
    }

    char **errors = (char **)aml_pool_alloc(ctx->pool, count * sizeof(char *));
    error = ctx->errors;
    for (size_t i = 0; i < count; i++) {
        errors[i] = error->message;
        error = error->next;
    }

    *num_errors = count;
    return errors;
}

char **sql_ctx_get_warnings(sql_ctx_t *ctx, size_t *num_warnings) {
    if (!ctx) return NULL;

    size_t count = 0;
    sql_ctx_message_t *warning = ctx->warnings;
    while (warning) {
        count++;
        warning = warning->next;
    }

    if (count == 0) {
        *num_warnings = 0;
        return NULL;
    }

    char **warnings = (char **)aml_pool_alloc(ctx->pool, count * sizeof(char *));
    warning = ctx->warnings;
    for (size_t i = 0; i < count; i++) {
        warnings[i] = warning->message;
        warning = warning->next;
    }

    *num_warnings = count;
    return warnings;
}

void sql_ctx_clear_messages(sql_ctx_t *ctx) {
    ctx->errors = NULL;
    ctx->warnings = NULL;
}

void sql_ctx_print_messages(sql_ctx_t *ctx) {
    if (!ctx) return;

    sql_ctx_message_t *error = ctx->errors;
    while (error) {
        printf("ERROR: %s\n", error->message);
        error = error->next;
    }

    sql_ctx_message_t *warning = ctx->warnings;
    while (warning) {
        printf("WARNING: %s\n", warning->message);
        warning = warning->next;
    }
}

void sql_ctx_register_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec) {
    if (!ctx || !spec) return;

    sql_ctx_spec_node_t *new_spec = (sql_ctx_spec_node_t *)aml_pool_alloc(ctx->pool, sizeof(sql_ctx_spec_node_t));
    new_spec->spec = spec;
    sql_ctx_spec_insert(&ctx->specs, new_spec);
}

sql_ctx_spec_t *sql_ctx_get_spec(sql_ctx_t *ctx, const char *name) {
    if (!ctx || !name) return NULL;

    sql_ctx_spec_node_t *spec_node = sql_ctx_spec_find(ctx->specs, name);
    if (!spec_node) return NULL;
    return spec_node->spec;
}
