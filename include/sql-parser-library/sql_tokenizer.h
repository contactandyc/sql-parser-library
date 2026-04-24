// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef _sql_tokenizer_h
#define _sql_tokenizer_h

#include "a-memory-library/aml_pool.h"
#include "sql-parser-library/sql_ctx.h"
#include <stdbool.h>
#include <stdint.h>

struct sql_ctx_function_spec_s;
typedef struct sql_ctx_function_spec_s sql_ctx_function_spec_t;

/**
 * Represents a raw SQL lexical token.
 */
typedef struct sql_token_s {
    sql_token_type_t type;    // Discovered type
    char *token;              // Extracted string value
    sql_ctx_spec_t *spec;     // Linked function spec (if applicable)
    const char *start;        // Original char pointer
    size_t start_position;    // Global offset
    size_t length;            // Token length
    size_t id;                // Unique ID
} sql_token_t;

/** Scans a raw SQL string and breaks it into an array of lexical tokens. */
sql_token_t **sql_tokenize(sql_ctx_t *context, const char *s, size_t *token_count);

/** Pretty-prints the token array for debugging. */
void sql_token_print(sql_token_t **tokens, size_t token_count);

#endif
