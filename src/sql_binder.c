// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_binder.h"
#include <string.h>
#include <strings.h>

#define MAX_TABLES_IN_QUERY 16

// A temporary struct to hold the mapping for the current query
typedef struct {
    const char *alias;
    const char *actual_table;
    int table_index;
} query_scope_t;

// Resolves a column identifier against the current query scope using the dynamic catalog
static sql_ctx_column_t *resolve_column(sql_ctx_t *context, query_scope_t *scope, size_t scope_count, const char *identifier) {
    const char *dot = strchr(identifier, '.');
    const char *table_part = NULL;
    const char *col_part = identifier;
    size_t table_len = 0;

    if (dot) {
        table_part = identifier;
        table_len = dot - identifier;
        col_part = dot + 1;
    }

    const char *target_actual_table = NULL;
    const char *target_alias = NULL;
    int target_table_index = -1;

    // 1. Translate alias to actual table name using scope
    if (table_part) {
        for (size_t i = 0; i < scope_count; i++) {
            if (strncasecmp(scope[i].alias, table_part, table_len) == 0 && scope[i].alias[table_len] == '\0') {
                target_actual_table = scope[i].actual_table;
                target_alias = scope[i].alias;
                target_table_index = scope[i].table_index;
                break;
            }
        }
        if (!target_actual_table) {
            sql_ctx_error(context, "Unknown table alias '%.*s'", (int)table_len, table_part);
            return NULL;
        }
    }

    sql_ctx_column_t *found_col = NULL;
    int match_count = 0;

    // 2. Query the Dynamic Catalog
    if (!context->schema_lookup) {
        sql_ctx_error(context, "No schema lookup callback registered.");
        return NULL;
    }

    if (target_actual_table) {
        // We know exactly which table to ask (Pass the alias, not the physical table!)
        found_col = context->schema_lookup(context, target_alias, col_part);
        if (found_col) match_count++;
    } else if (scope_count > 0) {
        // No prefix provided. Ask EVERY table in scope to ensure no ambiguity.
        for (size_t s = 0; s < scope_count; s++) {
            // Pass the alias instead of the actual_table
            sql_ctx_column_t *col = context->schema_lookup(context, scope[s].alias, col_part);
            if (col) {
                found_col = col;
                target_table_index = scope[s].table_index;
                match_count++;
            }
        }
    } else {
        // Standalone raw expression with no tables in scope.
        found_col = context->schema_lookup(context, NULL, col_part);
        if (found_col) {
            target_table_index = 0;
            match_count++;
        }
    }

    // 3. Handle Errors
    if (match_count == 0) {
        sql_ctx_error(context, "Unknown column '%s'", identifier);
        return NULL;
    }

    if (match_count > 1) {
        sql_ctx_error(context, "Ambiguous column reference: '%s'", identifier);
        return NULL;
    }

    // 4. Inject execution index
    found_col->table_index = target_table_index;

    return found_col;
}

// Recursive AST walker for binding
static bool bind_ast_node(sql_ctx_t *ctx, query_scope_t *scope, size_t scope_count,
                          sql_ast_node_t *projection_list, sql_ast_node_t *node) {
    if (!node) return true;

    // --- COLUMN ALIAS INJECTION ---
    if (node->type == SQL_IDENTIFIER && projection_list) {
        if (!strchr(node->value, '.')) {
            sql_ast_node_t *proj = projection_list;
            while (proj) {
                if (proj->alias && strcasecmp(proj->alias, node->value) == 0) {
                    sql_ast_node_t *next_ptr = node->next;
                    char *orig_alias = node->value;    // Save the alias text
                    *node = *proj;
                    node->alias = orig_alias;          // Restore the alias text instead of zeroing!
                    node->next = next_ptr;
                    return bind_ast_node(ctx, scope, scope_count, NULL, node);
                }
                proj = proj->next;
            }
        }
    }

    // --- PHYSICAL SCHEMA BINDING ---
    if (node->type == SQL_IDENTIFIER) {
        sql_ctx_column_t *col = resolve_column(ctx, scope, scope_count, node->value);
        if (!col) return false;

        node->column = col;
        node->data_type = col->type;
    }

    bool ok = true;
    if (node->left)  ok &= bind_ast_node(ctx, scope, scope_count, projection_list, node->left);
    if (node->right) ok &= bind_ast_node(ctx, scope, scope_count, projection_list, node->right);
    if (node->next)  ok &= bind_ast_node(ctx, scope, scope_count, projection_list, node->next);

    return ok;
}

// Internal binding engine
static bool internal_bind_query(sql_ctx_t *ctx, sql_select_t *query, bool allow_where_aliases) {
    if (!query) return false;

    // --- STEP 1: BUILD THE QUERY SCOPE ---
    query_scope_t scope[MAX_TABLES_IN_QUERY];
    size_t scope_count = 0;

    // Sync Base Table with Planner index logic
    if (query->table || query->subquery) {
        if (query->table) {
            scope[scope_count].actual_table = query->table;
            scope[scope_count].alias = query->table_alias ? query->table_alias : query->table;
        } else {
            scope[scope_count].actual_table = query->table_alias ? query->table_alias : "subquery";
            scope[scope_count].alias = query->table_alias ? query->table_alias : "subquery";
        }
        scope[scope_count].table_index = scope_count;
        scope_count++;
    }

    // Sync Join Tables with Planner index logic
    sql_join_t *j = query->joins;
    while (j && scope_count < MAX_TABLES_IN_QUERY) {
        if (j->table) {
            scope[scope_count].actual_table = j->table;
            scope[scope_count].alias = j->alias ? j->alias : j->table;
        } else {
            scope[scope_count].actual_table = j->alias ? j->alias : "subquery";
            scope[scope_count].alias = j->alias ? j->alias : "subquery";
        }
        scope[scope_count].table_index = scope_count;
        scope_count++;
        j = j->next;
    }

    // --- STEP 2: BIND ALL CLAUSES ---
    bool ok = true;

    ok &= bind_ast_node(ctx, scope, scope_count, NULL, query->columns);

    j = query->joins;
    while (j) {
        ok &= bind_ast_node(ctx, scope, scope_count, NULL, j->on_condition);
        j = j->next;
    }

    sql_ast_node_t *where_projections = allow_where_aliases ? query->columns : NULL;
    ok &= bind_ast_node(ctx, scope, scope_count, where_projections, query->where_clause);

    ok &= bind_ast_node(ctx, scope, scope_count, query->columns, query->group_by);
    ok &= bind_ast_node(ctx, scope, scope_count, query->columns, query->having_clause);

    sql_order_by_t *ob = query->order_by;
    while (ob) {
        ok &= bind_ast_node(ctx, scope, scope_count, query->columns, ob->expr);
        ob = ob->next;
    }

    ok &= bind_ast_node(ctx, scope, scope_count, NULL, query->limit);
    ok &= bind_ast_node(ctx, scope, scope_count, NULL, query->offset);

    return ok;
}

// --- PUBLIC API ---
bool sql_bind_query_strict(sql_ctx_t *ctx, sql_select_t *query) {
    return internal_bind_query(ctx, query, false);
}

bool sql_bind_query_extended(sql_ctx_t *ctx, sql_select_t *query) {
    return internal_bind_query(ctx, query, true);
}

bool sql_bind_expression(sql_ctx_t *ctx, sql_ast_node_t *expr) {
    if (!expr) return true;
    return bind_ast_node(ctx, NULL, 0, NULL, expr);
}
