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

// SQL Token Structure
typedef struct sql_token_s {
    sql_token_type_t type;    // Token type
    char *token;              // Token value
    sql_ctx_spec_t *spec; // Function specification (only used to indicate existence of a function)
    const char *start;        // Pointer in sql string where token came from
    size_t start_position;    // Start position in the input string
    size_t length;            // Length of the token
    size_t id;                // Unique ID for each token
} sql_token_t;

// Function declarations
sql_token_t **sql_tokenize(sql_ctx_t *context, const char *s, size_t *token_count);

// Print an array of tokens
void sql_token_print(sql_token_t **tokens, size_t token_count);

#endif
