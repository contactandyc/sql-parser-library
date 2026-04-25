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
struct sql_select_s; // Forward declaration for subqueries

/**
 * Represents a single ORDER BY expression and its direction.
 */
typedef struct sql_order_by_s {
    struct sql_ast_node_s *expr;
    bool is_desc;
    struct sql_order_by_s *next;
} sql_order_by_t;

/**
 * Represents a WINDOW clause (OVER) attached to an aggregate/window function.
 */
typedef struct sql_window_s {
    struct sql_ast_node_s *partition_by; // Linked list of partition expressions
    sql_order_by_t *order_by;            // Order by rules for the window
} sql_window_t;

/**
 * The raw Abstract Syntax Tree node.
 * This represents the purely textual layout of the query before it is
 * bound to a schema or compiled into executable VM nodes.
 */
typedef struct sql_ast_node_s {
    sql_token_type_t type;       // Type of the AST node (matches token type)
    char *value;                 // Raw string value (e.g., column name or literal)
    char *alias;                 // Optional AS alias
    sql_data_type_t data_type;   // Discovered data type (if literal)
    sql_ctx_spec_t *spec;        // Function spec (if a known function)
    sql_ctx_column_t *column;    // Populated during the Binding phase

    sql_window_t *window_clause; // Window definitions (for OVER clauses)

    // --- Expression Subqueries ---
    struct sql_select_s *subquery; // Populated if type == SQL_NODE_SUBQUERY

    struct sql_ast_node_s *left;   // Left child (e.g., LHS of a binary operator)
    struct sql_ast_node_s *right;  // Right child (e.g., RHS of a binary operator)
    struct sql_ast_node_s *next;   // Sibling node (e.g., next item in a SELECT list)
} sql_ast_node_t;

/** Builds an AST expression tree from a list of tokens. */
sql_ast_node_t *sql_build_ast(sql_ctx_t *context, sql_token_t **tokens, size_t token_count);

/** Pretty-prints the AST to stdout for debugging. */
void sql_print_ast(sql_ast_node_t *node, int depth);

/** Parses a single expression, consuming tokens and updating the pos pointer. */
sql_ast_node_t *sql_parse_expression(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);

#endif /* _sql_ast_H */