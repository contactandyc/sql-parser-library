// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef _sql_query_H
#define _sql_query_H

#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_ast.h"
#include <stdbool.h>

typedef enum {
    JOIN_INNER,
    JOIN_LEFT,
    JOIN_RIGHT,
    JOIN_FULL
} sql_join_type_t;

struct sql_select_s;

typedef struct sql_cte_s {
    char *alias;
    struct sql_select_s *query;
    struct sql_cte_s *next;
} sql_cte_t;

typedef struct sql_join_s {
    sql_join_type_t type;
    char *table;
    struct sql_select_s *subquery;
    char *alias;
    sql_ast_node_t *on_condition;
    struct sql_join_s *next;
} sql_join_t;

/**
 * The top-level representation of a parsed SQL statement.
 */
typedef struct sql_select_s {
    bool is_explain;
    sql_cte_t *ctes;
    bool is_star;
    sql_ast_node_t *columns;

    char *table;
    struct sql_select_s *subquery;
    char *table_alias;
    sql_join_t *joins;

    sql_ast_node_t *where_clause;
    sql_ast_node_t *group_by;
    sql_ast_node_t *having_clause;
    sql_order_by_t *order_by;
    sql_ast_node_t *limit;
    sql_ast_node_t *offset;
} sql_select_t;

/** Used internally by the AST builder to intercept nested subqueries. */
sql_select_t *sql_parse_select_query(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t token_count);

/** Primary entry point: Parses a token array into a unified query structure. */
sql_select_t *sql_parse_query(sql_ctx_t *context, sql_token_t **tokens, size_t token_count);

/** Pretty-prints the query structure for debugging. */
void sql_print_query(sql_select_t *query, int depth);

/** Translates the Query structure back into a standardized SQL string. */
char *sql_query_to_string(sql_ctx_t *context, sql_select_t *query);

/** Translates an AST node back into a standardized SQL string. */
char *sql_ast_to_string(sql_ctx_t *ctx, sql_ast_node_t *node);

#endif /* _sql_query_H */
