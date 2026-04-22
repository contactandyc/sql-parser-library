// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ast.h"
#include "sql-parser-library/date_utils.h"
#include "a-memory-library/aml_pool.h"
#include <strings.h>
#include <string.h>
#include <ctype.h>

static inline bool is_context_error(sql_ctx_t *context) {
    return context->errors;
}

sql_ast_node_t *create_ast_node(sql_ctx_t *context, sql_token_t *token) {
    sql_ast_node_t *node = (sql_ast_node_t *)aml_pool_zalloc(context->pool, sizeof(sql_ast_node_t));
    node->type = token->type;
    node->value = token->token ? aml_pool_strdup(context->pool, token->token) : NULL;
    node->spec = token->spec;
    node->left = NULL;
    node->right = NULL;
    node->next = NULL;

    switch (token->type) {
        case SQL_IDENTIFIER: {
            // Check for booleans first
            if (!strcasecmp(token->token, "TRUE") || !strcasecmp(token->token, "FALSE")) {
                node->type = SQL_LITERAL;
                node->data_type = SQL_TYPE_BOOL;
            } else {
                // It's a raw identifier. We do NOT know what data type it is yet,
                // and we do NOT link the column. The Binder will do this later.
                node->data_type = SQL_TYPE_UNKNOWN;
                node->column = NULL;
            }
            break;
        }
        case SQL_NUMBER:
            if (strchr(token->token, '.') != NULL) {
                node->data_type = SQL_TYPE_DOUBLE;
            } else {
                node->data_type = SQL_TYPE_INT;
            }
            break;
        case SQL_COMPOUND_LITERAL:
            if (!strncasecmp(token->token, "TIMESTAMP", 9)) {
                time_t epoch = 0;

                const char *val_ptr = token->token + 9;
                while (isspace(*val_ptr)) val_ptr++;
                if (*val_ptr == '\'') val_ptr++;

                char *clean_val = aml_pool_strdup(context->pool, val_ptr);
                if (strlen(clean_val) > 0 && clean_val[strlen(clean_val)-1] == '\'') {
                    clean_val[strlen(clean_val)-1] = '\0';
                }

                if (convert_string_to_datetime(&epoch, context->pool, clean_val)) {
                    node->data_type = SQL_TYPE_DATETIME;
                } else {
                    sql_ctx_error(context, "Invalid timestamp format: %s", token->token);
                    node->data_type = SQL_TYPE_STRING;
                }
            } else {
                node->data_type = SQL_TYPE_STRING;
            }
            break;
        case SQL_LITERAL:
            node->data_type = SQL_TYPE_STRING;
            break;
        case SQL_COMPARISON:
        case SQL_AND:
        case SQL_OR:
        case SQL_NOT:
            node->data_type = SQL_TYPE_BOOL;
            break;
        default:
            node->data_type = SQL_TYPE_UNKNOWN;
            break;
    }

    return node;
}

/* Forward declarations */
sql_ast_node_t *parse_expression(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_and_expression(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_unary(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_comparison(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);

sql_ast_node_t *parse_bitwise_or(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_bitwise_and(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_bitwise_shift(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_concatenation(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_arithmetic_expression(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_term(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_exponent(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_factor(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_json_accessor(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_primary(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);

sql_ast_node_t *parse_function_call(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_in_list(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t token_count);


/* ------------------------------------------------------------------
 * Hierarchy Engine
 * ------------------------------------------------------------------ */

sql_ast_node_t *parse_bitwise_or(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_ast_node_t *left = parse_bitwise_and(context, tokens, pos, end_pos);
    if (is_context_error(context)) return NULL;

    while (*pos < end_pos && tokens[*pos]->type == SQL_OPERATOR && tokens[*pos]->token[0] == '|' && tokens[*pos]->token[1] == '\0') {
        sql_token_t *operator_token = tokens[(*pos)++];
        sql_ast_node_t *operator_node = create_ast_node(context, operator_token);
        if (is_context_error(context)) return NULL;

        operator_node->left = left;
        operator_node->right = parse_bitwise_and(context, tokens, pos, end_pos);
        if (is_context_error(context)) return NULL;
        left = operator_node;
    }
    return left;
}

sql_ast_node_t *parse_bitwise_and(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_ast_node_t *left = parse_bitwise_shift(context, tokens, pos, end_pos);
    if (is_context_error(context)) return NULL;

    while (*pos < end_pos && tokens[*pos]->type == SQL_OPERATOR && tokens[*pos]->token[0] == '&' && tokens[*pos]->token[1] == '\0') {
        sql_token_t *operator_token = tokens[(*pos)++];
        sql_ast_node_t *operator_node = create_ast_node(context, operator_token);
        if (is_context_error(context)) return NULL;

        operator_node->left = left;
        operator_node->right = parse_bitwise_shift(context, tokens, pos, end_pos);
        if (is_context_error(context)) return NULL;
        left = operator_node;
    }
    return left;
}

sql_ast_node_t *parse_bitwise_shift(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_ast_node_t *left = parse_concatenation(context, tokens, pos, end_pos);
    if (is_context_error(context)) return NULL;

    while (*pos < end_pos && tokens[*pos]->type == SQL_OPERATOR &&
           ((tokens[*pos]->token[0] == '<' && tokens[*pos]->token[1] == '<') ||
            (tokens[*pos]->token[0] == '>' && tokens[*pos]->token[1] == '>'))) {
        sql_token_t *operator_token = tokens[(*pos)++];
        sql_ast_node_t *operator_node = create_ast_node(context, operator_token);
        if (is_context_error(context)) return NULL;

        operator_node->left = left;
        operator_node->right = parse_concatenation(context, tokens, pos, end_pos);
        if (is_context_error(context)) return NULL;
        left = operator_node;
    }
    return left;
}

sql_ast_node_t *parse_concatenation(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_ast_node_t *left = parse_arithmetic_expression(context, tokens, pos, end_pos);
    if (is_context_error(context)) return NULL;

    while (*pos < end_pos && tokens[*pos]->type == SQL_OPERATOR &&
           tokens[*pos]->token[0] == '|' && tokens[*pos]->token[1] == '|') {
        sql_token_t *operator_token = tokens[(*pos)++];
        sql_ast_node_t *operator_node = create_ast_node(context, operator_token);
        if (is_context_error(context)) return NULL;

        operator_node->left = left;
        operator_node->right = parse_arithmetic_expression(context, tokens, pos, end_pos);
        if (is_context_error(context)) return NULL;
        left = operator_node;
    }
    return left;
}

sql_ast_node_t *parse_arithmetic_expression(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_ast_node_t *left = parse_term(context, tokens, pos, end_pos);
    if (is_context_error(context)) return NULL;

    while (*pos < end_pos && (tokens[*pos]->type == SQL_OPERATOR) &&
           ((tokens[*pos]->token[0] == '+' && tokens[*pos]->token[1] == '\0') ||
            (tokens[*pos]->token[0] == '-' && tokens[*pos]->token[1] == '\0'))) {
        sql_token_t *operator_token = tokens[(*pos)++];
        sql_ast_node_t *operator_node = create_ast_node(context, operator_token);
        if (is_context_error(context)) return NULL;

        operator_node->left = left;
        operator_node->right = parse_term(context, tokens, pos, end_pos);
        if (is_context_error(context)) return NULL;
        left = operator_node;
    }

    return left;
}

sql_ast_node_t *parse_term(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_ast_node_t *left = parse_exponent(context, tokens, pos, end_pos);
    if (is_context_error(context)) return NULL;

    while (*pos < end_pos && (tokens[*pos]->type == SQL_OPERATOR) &&
           (tokens[*pos]->token[0] == '*' || tokens[*pos]->token[0] == '/' || tokens[*pos]->token[0] == '%')) {
        sql_token_t *operator_token = tokens[(*pos)++];
        sql_ast_node_t *operator_node = create_ast_node(context, operator_token);
        if (is_context_error(context)) return NULL;

        operator_node->left = left;
        operator_node->right = parse_exponent(context, tokens, pos, end_pos);
        if (is_context_error(context)) return NULL;
        left = operator_node;
    }

    return left;
}

sql_ast_node_t *parse_exponent(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_ast_node_t *left = parse_factor(context, tokens, pos, end_pos);
    if (is_context_error(context)) return NULL;

    while (*pos < end_pos && tokens[*pos]->type == SQL_OPERATOR && tokens[*pos]->token[0] == '^') {
        sql_token_t *operator_token = tokens[(*pos)++];
        sql_ast_node_t *operator_node = create_ast_node(context, operator_token);
        if (is_context_error(context)) return NULL;

        operator_node->left = left;
        operator_node->right = parse_factor(context, tokens, pos, end_pos);
        if (is_context_error(context)) return NULL;
        left = operator_node;
    }
    return left;
}

sql_ast_node_t *parse_factor(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    if (*pos < end_pos && (tokens[*pos]->type == SQL_OPERATOR) &&
        ((tokens[*pos]->token[0] == '+' && tokens[*pos]->token[1] == '\0') ||
         (tokens[*pos]->token[0] == '-' && tokens[*pos]->token[1] == '\0') ||
         (tokens[*pos]->token[0] == '~' && tokens[*pos]->token[1] == '\0'))) {
        sql_token_t *unary_op = tokens[(*pos)++];
        sql_ast_node_t *node = create_ast_node(context, unary_op);
        if (is_context_error(context)) return NULL;

        node->left = parse_factor(context, tokens, pos, end_pos);
        if (is_context_error(context)) return NULL;
        return node;
    }

    return parse_json_accessor(context, tokens, pos, end_pos);
}

sql_ast_node_t *parse_json_accessor(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_ast_node_t *left = parse_primary(context, tokens, pos, end_pos);
    if (is_context_error(context)) return NULL;

    while (*pos < end_pos && tokens[*pos]->type == SQL_OPERATOR &&
           strcmp(tokens[*pos]->token, "->>") == 0) {
        sql_token_t *operator_token = tokens[(*pos)++];
        sql_ast_node_t *operator_node = create_ast_node(context, operator_token);
        if (is_context_error(context)) return NULL;

        operator_node->left = left;
        operator_node->right = parse_primary(context, tokens, pos, end_pos);
        if (is_context_error(context)) return NULL;
        left = operator_node;
    }
    return left;
}

/* ------------------------------------------------------------------
 * Primary, function calls, etc.
 * ------------------------------------------------------------------ */

size_t find_argument_end(sql_ctx_t *context, sql_token_t **tokens, size_t pos,
                         size_t end_pos, sql_token_type_t closing_token_type) {
    int paren_level = 0;
    int bracket_level = 0;
    size_t current_pos = pos;

    while (current_pos < end_pos) {
        sql_token_t *token = tokens[current_pos];

        if (token->type == SQL_OPEN_PAREN) {
            paren_level++;
        } else if (token->type == SQL_CLOSE_PAREN) {
            if (paren_level > 0) {
                paren_level--;
            } else {
                if (closing_token_type == SQL_CLOSE_PAREN) {
                    break;
                } else {
                    sql_ctx_error(context, "Unexpected closing parenthesis");
                    break;
                }
            }
        } else if (token->type == SQL_OPEN_BRACKET) {
            bracket_level++;
        } else if (token->type == SQL_CLOSE_BRACKET) {
            if (bracket_level > 0) {
                bracket_level--;
            } else {
                if (closing_token_type == SQL_CLOSE_BRACKET) {
                    break;
                } else {
                    sql_ctx_error(context, "Unexpected closing bracket");
                    break;
                }
            }
        } else if (token->type == SQL_COMMA) {
            if (paren_level == 0 && bracket_level == 0) {
                break;
            }
        }
        current_pos++;
    }
    return current_pos;
}

sql_ast_node_t *parse_primary(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    if (*pos >= end_pos) {
        sql_ctx_error(context, "Unexpected end of tokens in parse_primary");
        return NULL;
    }

    sql_token_t *token = tokens[*pos];

    if (strcasecmp(token->token, "CASE") == 0) {
        (*pos)++;
        sql_ast_node_t *case_node = create_ast_node(context, token);
        case_node->type = SQL_FUNCTION;
        case_node->spec = sql_ctx_get_spec(context, "CASE");

        sql_ast_node_t *head = NULL, *tail = NULL;

        while (*pos < end_pos && strcasecmp(tokens[*pos]->token, "END") != 0) {
            sql_ast_node_t *clause_node = NULL;
            if (strcasecmp(tokens[*pos]->token, "WHEN") == 0) {
                sql_token_t *when_token = tokens[(*pos)++];
                clause_node = create_ast_node(context, when_token);
                clause_node->left = parse_expression(context, tokens, pos, end_pos);
                if (*pos < end_pos && strcasecmp(tokens[*pos]->token, "THEN") == 0) {
                    (*pos)++;
                    clause_node->right = parse_expression(context, tokens, pos, end_pos);
                } else {
                    sql_ctx_error(context, "Expected THEN after WHEN");
                    return NULL;
                }
            } else if (strcasecmp(tokens[*pos]->token, "ELSE") == 0) {
                sql_token_t *else_token = tokens[(*pos)++];
                clause_node = create_ast_node(context, else_token);
                clause_node->left = parse_expression(context, tokens, pos, end_pos);
            } else {
                sql_ctx_error(context, "Expected WHEN, ELSE, or END inside CASE");
                return NULL;
            }

            if (!head) { head = tail = clause_node; }
            else { tail->next = clause_node; tail = clause_node; }
        }

        if (*pos < end_pos && strcasecmp(tokens[*pos]->token, "END") == 0) {
            (*pos)++;
        } else {
            sql_ctx_error(context, "Expected END to close CASE");
            return NULL;
        }

        case_node->left = head;
        return case_node;
    }

    if (token->type == SQL_OPEN_PAREN) {
        (*pos)++;
        sql_ast_node_t *node = parse_expression(context, tokens, pos, end_pos);
        if (is_context_error(context)) return NULL;
        if (*pos < end_pos && tokens[*pos]->type == SQL_CLOSE_PAREN) {
            (*pos)++;
        } else {
            sql_ctx_error(context, "Expected closing parenthesis in parse_primary");
        }
        return node;
    }

    if (token->type == SQL_FUNCTION) {
        (*pos)++;
        return parse_function_call(context, tokens, pos, end_pos);
    }

    if (token->type == SQL_IDENTIFIER ||
        token->type == SQL_COMPOUND_LITERAL ||
        token->type == SQL_LITERAL ||
        token->type == SQL_NUMBER) {
        sql_ast_node_t *node = create_ast_node(context, token);
        if (is_context_error(context)) return NULL;
        (*pos)++;

        if (*pos < end_pos && tokens[*pos]->type == SQL_OPERATOR &&
            strcmp(tokens[*pos]->token, "::") == 0) {
            (*pos)++;

            if (*pos < end_pos &&
                (tokens[*pos]->type == SQL_KEYWORD ||
                 tokens[*pos]->type == SQL_IDENTIFIER ||
                 tokens[*pos]->type == SQL_FUNCTION)) {
                sql_ast_node_t *cast_type = create_ast_node(context, tokens[*pos]);
                if (is_context_error(context)) return NULL;
                (*pos)++;

                sql_ast_node_t *cast_node = create_ast_node(context, &(sql_token_t){SQL_FUNCTION, "::"});
                if (is_context_error(context)) return NULL;
                cast_node->spec = sql_ctx_get_spec(context, "::");
                cast_node->left = node;
                cast_node->right = cast_type;
                return cast_node;
            } else {
                sql_ctx_error(context, "Expected type identifier after '::'");
                return NULL;
            }
        }

        return node;
    }

    sql_ctx_error(context, "Unexpected token in parse_primary: %s", token->token);
    return NULL;
}

sql_ast_node_t *parse_function_call(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_token_t *func_name_token = tokens[*pos - 1];
    sql_ast_node_t *func_node = create_ast_node(context, func_name_token);
    if (is_context_error(context)) return NULL;
    func_node->type = SQL_FUNCTION;

    if (*pos < end_pos && tokens[*pos]->type == SQL_OPEN_PAREN) {
        (*pos)++;

        if (strcasecmp(func_name_token->token, "EXTRACT") == 0) {
            sql_ast_node_t *field_node = NULL;
            if (*pos < end_pos) {
                field_node = create_ast_node(context, tokens[(*pos)++]);
            }

            if (*pos < end_pos && strcasecmp(tokens[*pos]->token, "FROM") == 0) {
                (*pos)++;
            } else {
                sql_ctx_error(context, "Expected FROM in EXTRACT function");
                return NULL;
            }

            sql_ast_node_t *source_node = parse_expression(context, tokens, pos, end_pos);

            if (*pos < end_pos && tokens[*pos]->type == SQL_CLOSE_PAREN) {
                (*pos)++;
            } else {
                sql_ctx_error(context, "Expected closing parenthesis for EXTRACT");
                return NULL;
            }

            sql_ast_node_t *from_node = create_ast_node(context, &(sql_token_t){SQL_KEYWORD, "FROM"});
            from_node->left = field_node;
            from_node->right = source_node;
            func_node->left = from_node;
            return func_node;
        }

        if (strcasecmp(func_name_token->token, "CAST") == 0) {
            sql_ast_node_t *expr_node = parse_expression(context, tokens, pos, end_pos);

            if (*pos < end_pos && strcasecmp(tokens[*pos]->token, "AS") == 0) {
                sql_ast_node_t *as_node = create_ast_node(context, tokens[(*pos)++]); // Consume AS

                if (*pos < end_pos) {
                    sql_ast_node_t *type_node = create_ast_node(context, tokens[(*pos)++]); // Consume type

                    if (*pos < end_pos && tokens[*pos]->type == SQL_CLOSE_PAREN) {
                        (*pos)++; // Consume ')'

                        expr_node->next = as_node;
                        as_node->next = type_node;
                        func_node->left = expr_node;
                        return func_node;
                    } else {
                        sql_ctx_error(context, "Expected closing parenthesis for CAST");
                        return NULL;
                    }
                }
            }
            sql_ctx_error(context, "Expected AS in CAST function");
            return NULL;
        }

        if (strcasecmp(func_name_token->token, "POSITION") == 0) {
            sql_ast_node_t *substr_node = parse_bitwise_or(context, tokens, pos, end_pos);

            if (*pos < end_pos && strcasecmp(tokens[*pos]->token, "IN") == 0) {
                (*pos)++;
            } else {
                sql_ctx_error(context, "Expected IN in POSITION function");
                return NULL;
            }

            sql_ast_node_t *str_node = parse_expression(context, tokens, pos, end_pos);

            if (*pos < end_pos && tokens[*pos]->type == SQL_CLOSE_PAREN) {
                (*pos)++;
            } else {
                sql_ctx_error(context, "Expected closing parenthesis for POSITION");
                return NULL;
            }

            substr_node->next = str_node;
            func_node->left = substr_node;
            return func_node;
        }

        sql_ast_node_t *arg_list_head = NULL;
        sql_ast_node_t *arg_list_tail = NULL;

        while (*pos < end_pos) {
            if (tokens[*pos]->type == SQL_CLOSE_PAREN) {
                (*pos)++;
                break;
            }

            size_t arg_end = find_argument_end(context, tokens, *pos, end_pos, SQL_CLOSE_PAREN);
            if (is_context_error(context)) return NULL;

            size_t arg_pos = *pos;
            sql_ast_node_t *arg = parse_expression(context, tokens, &arg_pos, arg_end);
            if (!arg) {
                sql_ctx_error(context, "Error parsing function argument");
                return NULL;
            }

            *pos = arg_pos;

            if (!arg_list_head) {
                arg_list_head = arg;
                arg_list_tail = arg;
            } else {
                arg_list_tail->next = arg;
                arg_list_tail = arg;
            }

            if (*pos < end_pos && tokens[*pos]->type == SQL_COMMA) {
                (*pos)++;
            }
        }

        func_node->left = arg_list_head;
    } else {
        func_node->type = SQL_FUNCTION_LITERAL;
        func_node->data_type = SQL_TYPE_STRING;
    }

    return func_node;
}

/* ------------------------------------------------------------------
 * Comparison & special operators (BETWEEN, IN, etc.)
 * ------------------------------------------------------------------ */

static sql_ast_node_t *parse_between(sql_ctx_t *context, sql_ast_node_t *left,
                                     sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_ast_node_t *between_node = create_ast_node(context,
        &(sql_token_t){ .type = SQL_COMPARISON, .token = "BETWEEN" });
    if (is_context_error(context)) return NULL;

    between_node->data_type = SQL_TYPE_BOOL;
    between_node->left = left;

    sql_ast_node_t *lower_bound = parse_bitwise_or(context, tokens, pos, end_pos);
    if (!lower_bound) {
        sql_ctx_error(context, "Expected lower bound after 'BETWEEN'");
        return NULL;
    }

    if (*pos < end_pos && strcasecmp(tokens[*pos]->token, "AND") == 0) {
        (*pos)++;
    } else {
        sql_ctx_error(context, "Expected 'AND' in BETWEEN clause");
        return NULL;
    }

    sql_ast_node_t *upper_bound = parse_bitwise_or(context, tokens, pos, end_pos);
    if (!upper_bound) {
        sql_ctx_error(context, "Expected upper bound after 'AND' in BETWEEN");
        return NULL;
    }

    sql_ast_node_t *bounds_node = create_ast_node(context, &(sql_token_t){ .type = SQL_TOKEN, .token = NULL });
    if (is_context_error(context)) return NULL;

    bounds_node->left = lower_bound;
    bounds_node->right = upper_bound;

    between_node->right = bounds_node;
    between_node->spec = sql_ctx_get_spec(context, "BETWEEN");

    return between_node;
}

static sql_ast_node_t *parse_not_between(sql_ctx_t *context, sql_ast_node_t *left,
                                         sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_ast_node_t *not_between_node = create_ast_node(context,
        &(sql_token_t){ .type = SQL_COMPARISON, .token = "NOT BETWEEN" });
    if (is_context_error(context)) return NULL;
    not_between_node->data_type = SQL_TYPE_BOOL;
    not_between_node->left = left;

    sql_ast_node_t *lower_bound = parse_bitwise_or(context, tokens, pos, end_pos);
    if (!lower_bound) {
        sql_ctx_error(context, "Expected lower bound after 'NOT BETWEEN'");
        return NULL;
    }

    if (*pos < end_pos && strcasecmp(tokens[*pos]->token, "AND") == 0) {
        (*pos)++;
    } else {
        sql_ctx_error(context, "Expected 'AND' in NOT BETWEEN clause");
        return NULL;
    }

    sql_ast_node_t *upper_bound = parse_bitwise_or(context, tokens, pos, end_pos);
    if (!upper_bound) {
        sql_ctx_error(context, "Expected upper bound after 'AND' in NOT BETWEEN");
        return NULL;
    }

    sql_ast_node_t *bounds_node = create_ast_node(context, &(sql_token_t){ .type = SQL_TOKEN, .token = NULL });
    if (is_context_error(context)) return NULL;
    bounds_node->left = lower_bound;
    bounds_node->right = upper_bound;
    not_between_node->right = bounds_node;
    not_between_node->spec = sql_ctx_get_spec(context, "NOT BETWEEN");

    return not_between_node;
}

static sql_ast_node_t *parse_in_operator(sql_ctx_t *context, sql_ast_node_t *left,
                                         sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_ast_node_t *in_node = create_ast_node(context,
        &(sql_token_t){ .type = SQL_COMPARISON, .token = "IN" });
    if (is_context_error(context)) return NULL;
    in_node->data_type = SQL_TYPE_BOOL;
    in_node->left = left;
    in_node->right = parse_in_list(context, tokens, pos, end_pos);
    if (is_context_error(context)) return NULL;
    in_node->spec = sql_ctx_get_spec(context, "IN");
    return in_node;
}

static sql_ast_node_t *parse_standard_comparison(sql_ctx_t *context,
                                                 sql_ast_node_t *left,
                                                 sql_token_t *operator_token,
                                                 sql_token_t **tokens,
                                                 size_t *pos,
                                                 size_t end_pos)
{
    sql_ast_node_t *op_node = create_ast_node(context, operator_token);
    if (is_context_error(context)) return NULL;
    op_node->data_type = SQL_TYPE_BOOL;

    if (strcasecmp(operator_token->token, "IS") == 0) {
        if ((*pos + 1) < end_pos && strcasecmp(tokens[*pos]->token, "NOT") == 0) {
            if(strcasecmp(tokens[*pos + 1]->token, "NULL") == 0) {
                (*pos) += 2;
                op_node->value = aml_pool_strdup(context->pool, "IS NOT NULL");
                op_node->left = left;
                op_node->type = SQL_COMPARISON;
                op_node->spec = sql_ctx_get_spec(context, "IS NOT NULL");
                return op_node;
            } else if(strcasecmp(tokens[*pos + 1]->token, "FALSE") == 0) {
                (*pos) += 2;
                op_node->value = aml_pool_strdup(context->pool, "IS NOT FALSE");
                op_node->left = left;
                op_node->type = SQL_COMPARISON;
                op_node->spec = sql_ctx_get_spec(context, "IS NOT FALSE");
                return op_node;
            } else if(strcasecmp(tokens[*pos + 1]->token, "TRUE") == 0) {
                (*pos) += 2;
                op_node->value = aml_pool_strdup(context->pool, "IS NOT TRUE");
                op_node->left = left;
                op_node->type = SQL_COMPARISON;
                op_node->spec = sql_ctx_get_spec(context, "IS NOT TRUE");
                return op_node;
            } else if (strcasecmp(tokens[*pos + 1]->token, "DISTINCT") == 0 &&
                       (*pos + 2) < end_pos && strcasecmp(tokens[*pos + 2]->token, "FROM") == 0) {
                (*pos) += 3;
                op_node->value = aml_pool_strdup(context->pool, "IS NOT DISTINCT FROM");
                op_node->left = left;
                op_node->right = parse_bitwise_or(context, tokens, pos, end_pos);
                op_node->type = SQL_COMPARISON;
                op_node->spec = sql_ctx_get_spec(context, "IS NOT DISTINCT FROM");
                return op_node;
            } else {
                sql_ctx_error(context, "Invalid syntax after 'IS NOT'");
                return NULL;
            }
        } else if(*pos < end_pos) {
            if (strcasecmp(tokens[*pos]->token, "NULL") == 0) {
                (*pos)++;
                op_node->value = aml_pool_strdup(context->pool, "IS NULL");
                op_node->left = left;
                op_node->type = SQL_COMPARISON;
                op_node->spec = sql_ctx_get_spec(context, "IS NULL");
                return op_node;
            } else if(strcasecmp(tokens[*pos]->token, "FALSE") == 0) {
                (*pos)++;
                op_node->value = aml_pool_strdup(context->pool, "IS FALSE");
                op_node->left = left;
                op_node->type = SQL_COMPARISON;
                op_node->spec = sql_ctx_get_spec(context, "IS FALSE");
                return op_node;
            } else if(strcasecmp(tokens[*pos]->token, "TRUE") == 0) {
                (*pos)++;
                op_node->value = aml_pool_strdup(context->pool, "IS TRUE");
                op_node->left = left;
                op_node->type = SQL_COMPARISON;
                op_node->spec = sql_ctx_get_spec(context, "IS TRUE");
                return op_node;
            } else if (strcasecmp(tokens[*pos]->token, "DISTINCT") == 0 &&
                       (*pos + 1) < end_pos && strcasecmp(tokens[*pos + 1]->token, "FROM") == 0) {
                (*pos) += 2;
                op_node->value = aml_pool_strdup(context->pool, "IS DISTINCT FROM");
                op_node->left = left;
                op_node->right = parse_bitwise_or(context, tokens, pos, end_pos);
                op_node->type = SQL_COMPARISON;
                op_node->spec = sql_ctx_get_spec(context, "IS DISTINCT FROM");
                return op_node;
            } else {
                sql_ctx_error(context, "Invalid syntax after 'IS'");
                return NULL;
            }
        } else {
            sql_ctx_error(context, "Invalid syntax after 'IS'");
            return NULL;
        }
    }

    sql_ast_node_t *right = parse_bitwise_or(context, tokens, pos, end_pos);
    if (is_context_error(context)) return NULL;

    if (operator_token->token[0] == '>') {
        op_node->value[0] = '<';
        op_node->left = right;
        op_node->right = left;
    } else {
        op_node->left = left;
        op_node->right = right;
    }
    op_node->spec = sql_ctx_get_spec(context, op_node->value);

    return op_node;
}

static sql_ast_node_t *parse_not_comparison_expression(sql_ctx_t *context,
                                                       sql_ast_node_t *left,
                                                       sql_token_t **tokens,
                                                       size_t *pos,
                                                       size_t end_pos)
{
    size_t not_pos = *pos;
    (*pos)++;

    if (*pos < end_pos &&
        (tokens[*pos]->type == SQL_COMPARISON || tokens[*pos]->type == SQL_KEYWORD))
    {
        sql_token_t *operator_token = tokens[*pos];

        if (strcasecmp(operator_token->token, "BETWEEN") == 0) {
            (*pos)++;
            return parse_not_between(context, left, tokens, pos, end_pos);
        }
        else if (strcasecmp(operator_token->token, "LIKE") == 0)
        {
            char *combined_operator = aml_pool_strdupf(context->pool, "NOT %s", operator_token->token);
            sql_token_t not_operator_token = {
                .type = SQL_COMPARISON,
                .token = combined_operator
            };
            (*pos)++;

            sql_ast_node_t *not_node = create_ast_node(context, &not_operator_token);
            if (is_context_error(context)) return NULL;
            not_node->data_type = SQL_TYPE_BOOL;
            not_node->left = left;

            not_node->right = parse_bitwise_or(context, tokens, pos, end_pos);
            not_node->spec = sql_ctx_get_spec(context, combined_operator);
            if (is_context_error(context)) return NULL;

            return not_node;
        } else if (strcasecmp(operator_token->token, "IN") == 0) {
            (*pos)++;

            sql_ast_node_t *not_in_node = create_ast_node(context,
                &(sql_token_t){ .type = SQL_COMPARISON, .token = "NOT IN" });

            if (is_context_error(context)) return NULL;

            not_in_node->data_type = SQL_TYPE_BOOL;
            not_in_node->left = left;
            not_in_node->right = parse_in_list(context, tokens, pos, end_pos);
            if (is_context_error(context)) return NULL;

            not_in_node->spec = sql_ctx_get_spec(context, "NOT IN");

            return not_in_node;
        }
    }

    *pos = not_pos;
    return left;
}

sql_ast_node_t *parse_comparison(sql_ctx_t *context,
                                 sql_token_t **tokens,
                                 size_t *pos,
                                 size_t end_pos)
{
    sql_ast_node_t *left = parse_bitwise_or(context, tokens, pos, end_pos);
    if (is_context_error(context)) return NULL;

    if (*pos < end_pos) {
        if (tokens[*pos]->type == SQL_NOT) {
            return parse_not_comparison_expression(context, left, tokens, pos, end_pos);
        }

        bool is_comparison_keyword =
            tokens[*pos]->type == SQL_KEYWORD &&
            (strcasecmp(tokens[*pos]->token, "BETWEEN") == 0 ||
             strcasecmp(tokens[*pos]->token, "IN")      == 0 ||
             strcasecmp(tokens[*pos]->token, "IS")      == 0 ||
             strcasecmp(tokens[*pos]->token, "LIKE")    == 0);

        bool is_comparison_operator =
            tokens[*pos]->type == SQL_COMPARISON ||
            (tokens[*pos]->type == SQL_OPERATOR && strcmp(tokens[*pos]->token, "~") == 0);

        if (is_comparison_operator || is_comparison_keyword) {
            sql_token_t *operator_token = tokens[(*pos)++];
            if (strcasecmp(operator_token->token, "BETWEEN") == 0) {
                return parse_between(context, left, tokens, pos, end_pos);
            } else if (strcasecmp(operator_token->token, "IN") == 0) {
                return parse_in_operator(context, left, tokens, pos, end_pos);
            } else {
                return parse_standard_comparison(context, left, operator_token, tokens, pos, end_pos);
            }
        }
    }

    return left;
}

sql_ast_node_t *parse_unary(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    if (*pos < end_pos && tokens[*pos]->type == SQL_NOT) {
        sql_token_t *not_token = tokens[(*pos)++];
        sql_ast_node_t *not_node = create_ast_node(context, not_token);
        if (is_context_error(context)) return NULL;

        not_node->left = parse_unary(context, tokens, pos, end_pos);
        return not_node;
    }

    return parse_comparison(context, tokens, pos, end_pos);
}

sql_ast_node_t *parse_and_expression(sql_ctx_t *context,
                                     sql_token_t **tokens,
                                     size_t *pos,
                                     size_t end_pos)
{
    sql_ast_node_t *left = parse_unary(context, tokens, pos, end_pos);
    if (is_context_error(context)) return NULL;

    while (*pos < end_pos) {
        if (tokens[*pos]->type == SQL_AND) {
            sql_token_t *token = tokens[(*pos)++];
            sql_ast_node_t *node = create_ast_node(context, token);
            if (is_context_error(context)) return NULL;
            node->left = left;
            node->right = parse_unary(context, tokens, pos, end_pos);
            if (is_context_error(context)) return NULL;
            node->data_type = SQL_TYPE_BOOL;
            left = node;
        }
        else if (tokens[*pos]->type == SQL_OR ||
                 tokens[*pos]->type == SQL_CLOSE_PAREN)
        {
            break;
        }
        else {
            break;
        }
    }

    return left;
}

sql_ast_node_t *parse_expression(sql_ctx_t *context,
                                 sql_token_t **tokens,
                                 size_t *pos,
                                 size_t end_pos)
{
    sql_ast_node_t *left = parse_and_expression(context, tokens, pos, end_pos);
    if (is_context_error(context)) return NULL;

    while (*pos < end_pos) {
        if (tokens[*pos]->type == SQL_OR) {
            sql_token_t *token = tokens[(*pos)++];
            sql_ast_node_t *node = create_ast_node(context, token);
            if (is_context_error(context)) return NULL;
            node->left = left;
            node->right = parse_and_expression(context, tokens, pos, end_pos);
            if (is_context_error(context)) return NULL;
            node->data_type = SQL_TYPE_BOOL;
            left = node;
        }
        else if (tokens[*pos]->type == SQL_CLOSE_PAREN) {
            break;
        }
        else {
            break;
        }
    }

    return left;
}

sql_ast_node_t *parse_in_list(sql_ctx_t *context,
                              sql_token_t **tokens,
                              size_t *pos,
                              size_t token_count)
{
    sql_ast_node_t *list_node = (sql_ast_node_t *)aml_pool_zalloc(context->pool, sizeof(sql_ast_node_t));
    list_node->type = SQL_LIST;
    list_node->value = NULL;
    list_node->left = NULL;
    list_node->right = NULL;
    list_node->next = NULL;

    if (*pos < token_count &&
        (tokens[*pos]->type == SQL_OPEN_BRACKET || tokens[*pos]->type == SQL_OPEN_PAREN))
    {
        sql_token_type_t closing_token_type =
            (tokens[*pos]->type == SQL_OPEN_BRACKET) ? SQL_CLOSE_BRACKET : SQL_CLOSE_PAREN;
        (*pos)++;

        sql_ast_node_t *expr_list_head = NULL;
        sql_ast_node_t *expr_list_tail = NULL;

        while (*pos < token_count) {
            if (tokens[*pos]->type == closing_token_type) {
                (*pos)++;
                break;
            }

            size_t expr_end = find_argument_end(context, tokens, *pos, token_count, closing_token_type);
            if (is_context_error(context)) return NULL;

            size_t expr_pos = *pos;
            sql_ast_node_t *expr = parse_expression(context, tokens, &expr_pos, expr_end);
            if (!expr) {
                sql_ctx_error(context, "Error parsing expression in IN list");
                break;
            }

            *pos = expr_pos;

            if (!expr_list_head) {
                expr_list_head = expr;
                expr_list_tail = expr;
            } else {
                expr_list_tail->next = expr;
                expr_list_tail = expr;
            }

            if (*pos < token_count && tokens[*pos]->type == SQL_COMMA) {
                (*pos)++;
            }
        }

        list_node->left = expr_list_head;
    } else {
        sql_ctx_error(context, "Expected '(' or '[' after IN");
    }

    return list_node;
}

sql_ast_node_t *build_ast(sql_ctx_t *context, sql_token_t **tokens, size_t token_count) {
    if (token_count == 0) return NULL;
    size_t pos = 0;
    sql_ast_node_t *root = parse_expression(context, tokens, &pos, token_count);

    if (is_context_error(context)) return NULL;

    if (pos < token_count && tokens[pos]->type != SQL_SEMICOLON) {
        sql_ctx_error(context, "Unexpected token at end of expression: %s", tokens[pos]->token);
        return NULL;
    }

    return root;
}

void print_ast(sql_ast_node_t *node, int depth) {
    if (!node) return;

    for (int i = 0; i < depth; i++) printf("  ");
    const char *type_name = sql_token_type_name(node->type);
    const char *data_type_name = sql_data_type_name(node->data_type);

    if (node->value) {
        printf("[%s] %s (DataType: %s) %p\n", type_name, node->value, data_type_name, node->spec);
    } else {
        printf("[%s] (DataType: %s) %p\n", type_name, data_type_name, node->spec);
    }

    if (node->type == SQL_COMPARISON &&
        (node->value &&
         (strcasecmp(node->value, "BETWEEN") == 0 ||
          strcasecmp(node->value, "NOT BETWEEN") == 0)))
    {
        for (int i = 0; i < depth + 1; i++) printf("  ");
        printf("Expression:\n");
        print_ast(node->left, depth + 2);

        if (node->right) {
            sql_ast_node_t *bounds_node = node->right;
            for (int i = 0; i < depth + 1; i++) printf("  ");
            printf("Lower Bound:\n");
            print_ast(bounds_node->left, depth + 2);

            for (int i = 0; i < depth + 1; i++) printf("  ");
            printf("Upper Bound:\n");
            print_ast(bounds_node->right, depth + 2);
        }
    }
    else if (node->type == SQL_COMPARISON &&
             (node->value &&
              (strcasecmp(node->value, "IN") == 0 ||
               strcasecmp(node->value, "NOT IN") == 0)))
    {
        for (int i = 0; i < depth + 1; i++) printf("  ");
        printf("Expression:\n");
        print_ast(node->left, depth + 2);

        if (node->right && node->right->left) {
            for (int i = 0; i < depth + 1; i++) printf("  ");
            printf("Values:\n");
            sql_ast_node_t *value_node = node->right->left;
            print_ast(value_node, depth + 2);
        }
    }
    else if (node->left && node->right) {
        for (int i = 0; i < depth + 1; i++) printf("  ");
        printf("Left:\n");
        print_ast(node->left, depth + 2);

        for (int i = 0; i < depth + 1; i++) printf("  ");
        printf("Right:\n");
        print_ast(node->right, depth + 2);
    }
    else if (node->left) {
        print_ast(node->left, depth + 1);
    }

    if (node->next) {
        print_ast(node->next, depth);
    }
}
