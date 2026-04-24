// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef _sql_ast_H
#define _sql_ast_H

#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_node.h"
#include "sql-parser-library/sql_tokenizer.h"

struct sql_ast_node_s;
struct sql_select_s; // Forward declaration

typedef struct sql_order_by_s {
    struct sql_ast_node_s *expr;
    bool is_desc;
    struct sql_order_by_s *next;
} sql_order_by_t;

typedef struct sql_window_s {
    struct sql_ast_node_s *partition_by;
    sql_order_by_t *order_by;
} sql_window_t;

typedef struct sql_ast_node_s {
    sql_token_type_t type;
    char *value;
    char *alias;
    sql_data_type_t data_type;
    sql_ctx_spec_t *spec;
    sql_ctx_column_t *column;

    sql_window_t *window_clause;

    // --- NEW: Expression Subqueries ---
    struct sql_select_s *subquery;

    struct sql_ast_node_s *left;
    struct sql_ast_node_s *right;
    struct sql_ast_node_s *next;
} sql_ast_node_t;

sql_ast_node_t *build_ast(sql_ctx_t *context, sql_token_t **tokens, size_t token_count);
void print_ast(sql_ast_node_t *node, int depth);

sql_ast_node_t *parse_expression(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);

#endif /* _sql_ast_H */
