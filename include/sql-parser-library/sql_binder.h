// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef _sql_binder_H
#define _sql_binder_H

#include "sql-parser-library/sql_query.h"
#include "sql-parser-library/sql_ast.h"
#include "sql-parser-library/sql_ctx.h"
#include <stdbool.h>

/**
 * The Binder traverses the parsed AST and attempts to link raw string identifiers
 * (like "users.id") to physical schema columns using the context's catalog callbacks.
 */

/** * Binds the query using ANSI Strict Mode.
 * (Column aliases defined in SELECT are NOT allowed in the WHERE clause).
 */
bool sql_bind_query_strict(sql_ctx_t *ctx, sql_select_t *query);

/** * Binds the query using Custom Engine Mode.
 * (Column aliases defined in SELECT ARE allowed in the WHERE clause).
 */
bool sql_bind_query_extended(sql_ctx_t *ctx, sql_select_t *query);

/** Resolves column identifiers for a standalone expression tree. */
bool sql_bind_expression(sql_ctx_t *ctx, sql_ast_node_t *expr);

#endif /* _sql_binder_H */
