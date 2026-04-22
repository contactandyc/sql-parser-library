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

typedef struct sql_ast_node_s {
    sql_token_type_t type;
    char *value;
    char *alias;
    sql_data_type_t data_type;
    sql_ctx_spec_t *spec;     // Top level function specification (if applicable)
    sql_ctx_column_t *column;

    struct sql_ast_node_s *left;   // Left child (for binary operators)
    struct sql_ast_node_s *right;  // Right child (for binary operators)
    struct sql_ast_node_s *next;   // Next sibling (if needed)
} sql_ast_node_t;

sql_ast_node_t *build_ast(sql_ctx_t *context, sql_token_t **tokens, size_t token_count);
void print_ast(sql_ast_node_t *node, int depth);

sql_ast_node_t *parse_expression(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);

#endif /* _sql_ast_H */
