// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef _sql_node_H
#define _sql_node_H

#include <time.h>
#include <stdbool.h>
#include "a-memory-library/aml_pool.h"

typedef enum {
    SQL_TOKEN = 0,        // Generic token
    SQL_NUMBER = 10,      // Numeric literals
    SQL_OPERATOR = 20,    // Operators like +, -, *, /
    SQL_COMPARISON = 30,  // Comparison operators like <, >, =, etc.
    SQL_AND = 50,         // AND (optional for special handling)
    SQL_OR = 60,          // OR (optional for special handling)
    SQL_NOT = 65,         // NOT (optional for special handling)
    SQL_OPEN_PAREN = 90,  // (
    SQL_CLOSE_PAREN = 100, // )
    SQL_OPEN_BRACKET = 101, // [
    SQL_CLOSE_BRACKET = 102, // ]
    SQL_COMMA = 130,      // Commas for separating items
    SQL_SEMICOLON = 240,  // Statement terminator
    SQL_KEYWORD = 200,    // General SQL keyword
    SQL_FUNCTION = 255,   // SUM, COUNT, AVG, etc.
    SQL_FUNCTION_LITERAL = 256,   // Function without parentheses (e.g., CURRENT_TIMESTAMP)
    SQL_COMMENT = 260,    // Comments (--, /* */)
    SQL_IDENTIFIER = 219, // Identifiers
    SQL_LITERAL = 220,    // String literals
    SQL_COMPOUND_LITERAL = 221, // Compound literals (e.g., INTERVAL 1 DAY, TIMESTAMP 2021-01-01)
    SQL_STAR = 222,       // For '*'
    SQL_NULL = 223,       // For NULL
    SQL_LIST = 300,       // For list of expressions
} sql_token_type_t;

const char *sql_token_type_name(sql_token_type_t type);

typedef enum {
    SQL_TYPE_UNKNOWN,
    SQL_TYPE_INT,
    SQL_TYPE_STRING,
    SQL_TYPE_DOUBLE,
    SQL_TYPE_DATETIME,
    SQL_TYPE_BOOL,
    SQL_TYPE_FUNCTION,
    SQL_TYPE_CUSTOM
} sql_data_type_t;

const char *sql_data_type_name(sql_data_type_t type);

struct sql_node_s;
typedef struct sql_node_s sql_node_t;

struct sql_ctx_s;
struct sql_ctx_spec_s;

struct sql_ctx_column_s;
typedef struct sql_ctx_column_s sql_ctx_column_t;

// callback function to resolve a row
typedef sql_node_t * (*sql_node_cb)(struct sql_ctx_s *ctx, sql_node_t *f);

struct sql_node_s {
    sql_token_type_t token_type;      // Node type from AST parse
    char *token;           // Token value

    sql_token_type_t type;      // Node type (e.g., SQL_OPERATOR, SQL_COMPARISON)
    sql_node_cb func;      // Function pointer to evaluate the node (if applicable)
    sql_data_type_t data_type;  // Data type of the node
    struct sql_ctx_spec_s *spec;  // Function specification (if applicable)
    sql_ctx_column_t *column;
    bool is_null;
    union {
        bool bool_value;
        int int_value;
        double double_value;
        const char *string_value;
        time_t epoch;  // for date time
        void *custom;  // for custom data types (typically setup when func is assigned)
    } value;
    int agg_index;
    sql_node_t **parameters;
    size_t num_parameters;
};

#endif
