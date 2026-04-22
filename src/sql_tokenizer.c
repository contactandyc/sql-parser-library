// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_tokenizer.h"
#include "a-memory-library/aml_pool.h"
#include "a-memory-library/aml_buffer.h"
#include <ctype.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include <ctype.h>
#include <strings.h>

sql_token_t *_sql_token_init(aml_buffer_t *bh, aml_pool_t *pool, const char *start, size_t length,
                             sql_token_type_t type, const char *alt_token) {
    size_t token_length = length;
    if (alt_token)
        token_length = strlen(alt_token);
    sql_token_t *token = (sql_token_t *)aml_pool_zalloc(pool, sizeof(sql_token_t) + token_length + 1);
    token->type = type;
    token->token = (char *)(token + 1);
    if (alt_token) {
        memcpy(token->token, alt_token, token_length);
        token->token[token_length] = '\0';
    }
    else {
        memcpy(token->token, start, length);
        token->token[length] = '\0';
    }
    token->id = -1; // Temporary ID, set it later
    token->start = start; // Store start pointer for position calculation later
    token->length = length;
    aml_buffer_append(bh, &token, sizeof(sql_token_t *));
    return token;
}


void handle_timestamp(aml_buffer_t *bh, sql_ctx_t *context, const char *start,
                      size_t length, const char **s) {
    aml_pool_t *pool = context->pool;
    const char *peek = *s;

    // Peek ahead past whitespace
    while (isspace(*peek)) peek++;

    if (*peek == '\'') {
        // Quoted timestamp literal (e.g., TIMESTAMP '2020-01-01')
        const char *literal_start = peek + 1; // Skip opening quote
        peek++;
        while (*peek != '\'' && *peek) {
            peek++;
        }
        const char *literal_end = peek;
        if (*peek == '\'') {
            peek++; // Skip closing quote
        } else {
            sql_ctx_error(context, "Unterminated quoted timestamp literal");
            return;
        }

        char *timestamp_token = aml_pool_strdupf(pool, "TIMESTAMP %.*s",
            (int)(literal_end - literal_start), literal_start);

        _sql_token_init(bh, pool, start, strlen(timestamp_token),
                        SQL_COMPOUND_LITERAL, timestamp_token);

        *s = peek; // Update actual position safely
    } else {
        // Unquoted: It's just a column named "timestamp", NOT a compound literal
        if (sql_ctx_is_reserved_keyword(context, "TIMESTAMP")) {
            _sql_token_init(bh, pool, start, length, SQL_KEYWORD, NULL);
        } else {
            _sql_token_init(bh, pool, start, length, SQL_IDENTIFIER, NULL);
        }
    }
}

void handle_interval(aml_buffer_t *bh, sql_ctx_t *context, const char *start,
                     size_t length, const char **s) {
    aml_pool_t *pool = context->pool;
    // Handle INTERVAL followed by a literal or unquoted interval
    while (isspace(**s)) (*s)++; // Skip whitespace

    const char *literal_start = *s;
    if (**s == '\'') {
        // Quoted interval literal
        literal_start++; // Skip opening quote
        (*s)++;
        while (**s != '\'' && **s) {
            (*s)++;
        }
        const char *literal_end = *s;
        if (**s == '\'') {
            (*s)++; // Skip closing quote
        } else {
            sql_ctx_error(context, "Unterminated quoted interval literal");
            return;
        }

        // Use aml_pool_strdupf to combine INTERVAL and the literal
        char *interval_token = aml_pool_strdupf(pool, "INTERVAL %.*s",
            (int)(literal_end - literal_start), literal_start);

        _sql_token_init(bh, pool, start, strlen(interval_token),
                        SQL_COMPOUND_LITERAL, interval_token);
    } else {
        // Unquoted interval (e.g., INTERVAL 5 DAYS)
        const char *literal_end = *s;
        bool space_found = false;
        while (isalnum(*literal_end) || (!space_found && isspace(*literal_end))) {
            if (isspace(*literal_end)) {
                const char *space_start = literal_end;
                while (isspace(*literal_end)) {
                    space_found = true;
                    literal_end++;
                }
                if (!isdigit(*literal_start) || !isalpha(*literal_end)) {
                    literal_end = space_start;
                    break;
                }
                continue;
            }
            literal_end++;
        }

        // Use aml_pool_strdupf to combine INTERVAL and the literal
        char *interval_token = aml_pool_strdupf(pool, "INTERVAL %.*s",
            (int)(literal_end - literal_start), literal_start);

        _sql_token_init(bh, pool, start, strlen(interval_token),
                        SQL_COMPOUND_LITERAL, interval_token);
        *s = literal_end; // Update position
    }
}

// Helper function to handle identifiers and keywords
void handle_identifier_or_keyword(aml_buffer_t *bh, sql_ctx_t *context, const char **s) {
    const char *start = *s;

    while (isalnum(**s) || **s == '_' || **s == '.') {
        (*s)++;
    }

    while (isalnum(**s) || **s == '_') (*s)++;
    size_t length = *s - start;
    aml_pool_t *pool = context->pool;

    char *identifier = (char *)aml_pool_alloc(pool, length + 1);
    memcpy(identifier, start, length);
    identifier[length] = '\0';

    if (strcasecmp(identifier, "INTERVAL") == 0 && isspace(**s)) {
        handle_interval(bh, context, start, length, s);
    } else if(strcasecmp(identifier, "TIMESTAMP") == 0 && isspace(**s)) {
        handle_timestamp(bh, context, start, length, s);
    } else if (strcasecmp(identifier, "AND") == 0) {
        _sql_token_init(bh, pool, start, length, SQL_AND, NULL);
    } else if (strcasecmp(identifier, "OR") == 0) {
        _sql_token_init(bh, pool, start, length, SQL_OR, NULL);
    } else if (strcasecmp(identifier, "NOT") == 0) {
        _sql_token_init(bh, pool, start, length, SQL_NOT, NULL);
    } else if (strcasecmp(identifier, "NULL") == 0) {
       _sql_token_init(bh, pool, start, length, SQL_NULL, NULL);
    } else if (strcasecmp(identifier, "LIKE") == 0 ||
               strcasecmp(identifier, "IN") == 0 ||
               strcasecmp(identifier, "BETWEEN") == 0) {
        _sql_token_init(bh, pool, start, length, SQL_COMPARISON, NULL);
    } else if (sql_ctx_is_reserved_keyword(context, identifier)) {
        _sql_token_init(bh, pool, start, length, SQL_KEYWORD, NULL);
    } else {
        sql_ctx_spec_t *spec = sql_ctx_get_spec(context, identifier);
        if(spec) {
            sql_token_t *token = _sql_token_init(bh, pool, start, length, SQL_FUNCTION, NULL);
            token->spec = spec;
        } else {
            _sql_token_init(bh, pool, start, length, SQL_IDENTIFIER, NULL);
        }
    }
}

// Helper function to handle numeric literals
void handle_number(aml_buffer_t *bh, aml_pool_t *pool, const char **s) {
    const char *start = *s;
    bool seen_dot = false;
    bool seen_e = false;
    bool has_underscores = false;

    // Handle optional leading sign
    if (**s == '+' || **s == '-') {
        (*s)++;
    }

    // First pass: Check for underscores and parse the number
    const char *scan = *s;
    while (*scan) {
        if (isdigit(*scan)) {
            scan++;
        } else if (*scan == '.' && !seen_dot && !seen_e) {
            seen_dot = true;
            scan++;
        } else if ((*scan == 'E' || *scan == 'e') && !seen_e) {
            seen_e = true;
            scan++;
            if (*scan == '+' || *scan == '-') {
                scan++; // Consume optional sign after 'E'
            }
        } else if (*scan == '_') {
            has_underscores = true;
            scan++;
        } else {
            break;
        }
    }

    // If underscores are present, clean up the number
    size_t length = scan - start;
    if (has_underscores) {
        char *cleaned_number = aml_pool_alloc(pool, length + 1); // Temporary buffer
        size_t cleaned_length = 0;

        // Copy characters, skipping underscores
        for (const char *p = start; p < scan; p++) {
            if (*p != '_') {
                cleaned_number[cleaned_length++] = *p;
            }
        }
        cleaned_number[cleaned_length] = '\0';
        // Initialize the token with the cleaned number
        if (cleaned_number[0] == '+') {
            cleaned_number++;
            cleaned_length--;
        }
        _sql_token_init(bh, pool, start, length, SQL_NUMBER, cleaned_number);
    } else {
        char *alt_token = aml_pool_alloc(pool, length + 1);
        memcpy(alt_token, start, length);
        alt_token[length] = '\0';
        if (alt_token[0] == '+') {
            alt_token++;
        }
        _sql_token_init(bh, pool, start, length, SQL_NUMBER, alt_token);
    }

    // Update position
    *s = scan;
}

// Helper function to handle operators and comparisons
void handle_operator(aml_buffer_t *bh, aml_pool_t *pool, const char **s) {
    const char *start = *s;
    char ch = **s;

    if ((**s == ':' && (*s)[1] == ':')) {
        _sql_token_init(bh, pool, start, 2, SQL_OPERATOR, NULL);
        *s += 2;
    } else if (ch == '=' || ch == '>' || ch == '<' || ch == '!') {
        if(ch == '<' && (*s)[1] == '>') {
            _sql_token_init(bh, pool, start, 2, SQL_COMPARISON, "!=");
            *s += 2;
        } else if ((*s)[1] == '=') { // okay because *s[0] is one of =, >, <, !
            _sql_token_init(bh, pool, start, 2, SQL_COMPARISON, NULL);
            *s += 2;
        } else {
            _sql_token_init(bh, pool, start, 1, SQL_COMPARISON, NULL);
            (*s)++;
        }
    } else {
        _sql_token_init(bh, pool, start, 1, SQL_OPERATOR, NULL);
        (*s)++;
    }
}

// Helper function to handle parentheses, commas, and semicolons
void handle_special_character(aml_buffer_t *bh, aml_pool_t *pool, const char **s) {
    char ch = **s;
    sql_token_type_t type;
    // no default is needed because all cases are handled (calling function ensures that)
    switch (ch) {
        case '(': type = SQL_OPEN_PAREN; break;
        case ')': type = SQL_CLOSE_PAREN; break;
        case ',': type = SQL_COMMA; break;
        case ';': type = SQL_SEMICOLON; break;
        case '[': type = SQL_OPEN_BRACKET; break;
        case ']': type = SQL_CLOSE_BRACKET; break;
    }
    _sql_token_init(bh, pool, *s, 1, type, NULL);
    (*s)++;
}

// Helper function to handle string literals
void handle_string_literal(aml_buffer_t *bh, aml_pool_t *pool, const char **s) {
    const char *start = ++(*s); // Skip opening quote
    while (**s && (**s != '\'' || *(*s + 1) == '\'')) {
        if (**s == '\'' && *(*s + 1) == '\'') (*s)++; // Handle escaped quote
        (*s)++;
    }
    _sql_token_init(bh, pool, start, *s - start, SQL_LITERAL, NULL);
    if (**s == '\'') (*s)++; // Skip closing quote
}

void handle_dash_or_slash(aml_buffer_t *bh, aml_pool_t *pool, const char **s) {
    char ch = **s;
    const char *start = *s;

    if (ch == '-') {
        if ((*s)[1] == '-') { // Single-line comment
            *s += 2;
            while (**s && **s != '\n') (*s)++;
            _sql_token_init(bh, pool, start, *s - start, SQL_COMMENT, NULL);
        } else { // Treat as an operator
            _sql_token_init(bh, pool, start, 1, SQL_OPERATOR, NULL);
            (*s)++;
        }
    } else if (ch == '/') {
        if ((*s)[1] == '*') { // Multi-line comment
            *s += 2;
            while (**s && !(**s == '*' && (*s)[1] == '/')) (*s)++;
            if (**s) (*s) += 2; // Skip closing */
            _sql_token_init(bh, pool, start, *s - start, SQL_COMMENT, NULL);
        } else { // Treat as an operator
            _sql_token_init(bh, pool, start, 1, SQL_OPERATOR, NULL);
            (*s)++;
        }
    }
}

void handle_signed_number_or_operator(aml_buffer_t *bh, aml_pool_t *pool, const char **s, sql_token_t *last_token) {
    // CRITICAL FIX: Treat a minus sign following a comma as a negative number
    if ((**s == '-' || **s == '+') &&
        (isdigit((*s)[1]) || ((*s)[1] == '.' && isdigit((*s)[2]))) &&
        (!last_token || last_token->type == SQL_OPERATOR ||
         last_token->type == SQL_OPEN_PAREN || last_token->type == SQL_COMPARISON ||
         last_token->type == SQL_COMMA)) {
        handle_number(bh, pool, s);
    } else {
        handle_operator(bh, pool, s);
    }
}

// Main tokenizer function
sql_token_t **sql_tokenize(sql_ctx_t *context, const char *s, size_t *token_count) {
    aml_pool_t *pool = context->pool;
    aml_buffer_t *bh = aml_buffer_pool_init(pool, sizeof(sql_token_t *) * 16);
    sql_token_t *last_token = NULL;
    const char *original_start = s;

    while (*s) {
        if (isalpha(*s) || *s == '_') {
            handle_identifier_or_keyword(bh, context, &s);
        } else if (isdigit(*s)) {
            handle_number(bh, pool, &s);
        } else if (*s == '-' || *s == '+') {
            handle_signed_number_or_operator(bh, pool, &s, last_token);
        } else {
            switch (*s) {
                case '=': case '>': case '<': case '!': case '*': case '/': case ':':
                    handle_operator(bh, pool, &s);
                    break;

                case '(': case ')': case ',': case ';': case '[': case ']':
                    handle_special_character(bh, pool, &s);
                    break;

                case '\'':
                    handle_string_literal(bh, pool, &s);
                    break;

                case ' ': case '\t': case '\n': case '\r': // Whitespace
                    s++;
                    break;

                default:
                    sql_ctx_error(context, "Unknown character: %c", *s);
                    s++;
                    break;
            }
        }

        // Update last token
        if (aml_buffer_length(bh) > 0) {
            last_token = ((sql_token_t **)aml_buffer_end(bh))[-1];
        }
    }

    *token_count = aml_buffer_length(bh) / sizeof(sql_token_t *);

    // Assign IDs to tokens
    sql_token_t **tokens = (sql_token_t **)aml_buffer_data(bh);
    for (size_t i = 0; i < *token_count; i++) {
        sql_token_t *token = tokens[i];
        token->start_position = token->start - original_start;
        token->id = i;
        if (token->type == SQL_FUNCTION || token->type == SQL_COMPARISON || token->type == SQL_OPERATOR ||
            token->type == SQL_AND || token->type == SQL_OR || token->type == SQL_NOT) {
            token->spec = sql_ctx_get_spec(context, token->token);
        }
    }

    return tokens;
}

// Print tokens
void sql_token_print(sql_token_t **tokens, size_t token_count) {
    for (size_t i = 0; i < token_count; i++) {
        sql_token_t *token = tokens[i];
        if(token->spec)
            printf("%zu [%s] %s (%s)\n", token->id, sql_token_type_name(token->type), token->token,
                   token->spec->description);
        else
            printf("%zu [%s] %s\n", token->id, sql_token_type_name(token->type), token->token);
    }
}
