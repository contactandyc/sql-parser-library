// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ast.h"
#include "sql-parser-library/date_utils.h"
#include "a-memory-library/aml_pool.h"
#include <strings.h>

static inline bool is_context_error(sql_ctx_t *context) {
    return context->errors;
}

void add_child_node(sql_ast_node_t *parent, sql_ast_node_t *child) {
    if (!parent->left) {
        parent->left = child;
    } else {
        sql_ast_node_t *sibling = parent->left;
        while (sibling->next) {
            sibling = sibling->next;
        }
        sibling->next = child;
    }
}

sql_ctx_column_t *get_column(const char *column_name, sql_ctx_t *context) {
    for (size_t i = 0; i < context->column_count; i++) {
        if (strcasecmp(context->columns[i].name, column_name) == 0) {
            return context->columns+i;
        }
    }
    return NULL; // Column not found
}

sql_ast_node_t *create_ast_node(sql_ctx_t *context, sql_token_t *token) {
    sql_ast_node_t *node = (sql_ast_node_t *)aml_pool_alloc(context->pool, sizeof(sql_ast_node_t));
    node->type = token->type;
    node->value = token->token ? aml_pool_strdup(context->pool, token->token) : NULL;
    node->spec = token->spec;
    node->left = NULL;
    node->right = NULL;
    node->next = NULL;

    // Set the data type based on the token type
    switch (token->type) {
        case SQL_IDENTIFIER: {
            sql_ctx_column_t *column = get_column(token->token, context);
            if (!column) {
                if (!strcasecmp(token->token, "TRUE") || !strcasecmp(token->token, "FALSE")) {
                    node->type = SQL_LITERAL;
                    node->data_type = SQL_TYPE_BOOL;
                } else {
                    sql_ctx_warning(context, "Unknown column '%s'", token->token);
                }
            } else {
                node->data_type = column->type;
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
                if (convert_string_to_datetime(&epoch, context->pool, token->token+10)) {
                    sql_ctx_warning(context, "Valid timestamp: %s", token->token);
                    char *iso_utc = convert_epoch_to_iso_utc(context->pool, epoch);
                    node->value = iso_utc;
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

/* Forward declarations of the main parse functions */
sql_ast_node_t *parse_expression(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_and_expression(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_unary(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_comparison(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_arithmetic_expression(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_term(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_factor(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_primary(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);

sql_ast_node_t *parse_function_call(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos);
sql_ast_node_t *parse_in_list(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t token_count);

/* ------------------------------------------------------------------
 *  Arithmetic Expressions
 * ------------------------------------------------------------------ */

sql_ast_node_t *parse_arithmetic_expression(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    // Handle unary +/-
    if (*pos < end_pos && (tokens[*pos]->type == SQL_OPERATOR) &&
        (tokens[*pos]->token[0] == '+' || tokens[*pos]->token[0] == '-')) {
        sql_token_t *unary_op = tokens[(*pos)++];
        sql_ast_node_t *node = create_ast_node(context, unary_op);
        if (is_context_error(context))
            return NULL;
        node->left = parse_arithmetic_expression(context, tokens, pos, end_pos);
        if (is_context_error(context))
            return NULL;
        return node;
    }

    sql_ast_node_t *left = parse_term(context, tokens, pos, end_pos);
    if (is_context_error(context))
        return NULL;

    while (*pos < end_pos && (tokens[*pos]->type == SQL_OPERATOR) &&
           (tokens[*pos]->token[0] == '+' || tokens[*pos]->token[0] == '-')) {
        sql_token_t *operator_token = tokens[(*pos)++];
        sql_ast_node_t *operator_node = create_ast_node(context, operator_token);
        if (is_context_error(context))
            return NULL;
        operator_node->left = left;
        operator_node->right = parse_term(context, tokens, pos, end_pos);
        if (is_context_error(context))
            return NULL;
        left = operator_node;
    }

    return left;
}

sql_ast_node_t *parse_term(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_ast_node_t *left = parse_factor(context, tokens, pos, end_pos);
    if (is_context_error(context))
        return NULL;

    while (*pos < end_pos && (tokens[*pos]->type == SQL_OPERATOR) &&
           (tokens[*pos]->token[0] == '*' || tokens[*pos]->token[0] == '/')) {
        sql_token_t *operator_token = tokens[(*pos)++];
        sql_ast_node_t *operator_node = create_ast_node(context, operator_token);
        if (is_context_error(context))
            return NULL;
        operator_node->left = left;
        operator_node->right = parse_factor(context, tokens, pos, end_pos);
        if (is_context_error(context))
            return NULL;
        left = operator_node;
    }

    return left;
}

sql_ast_node_t *parse_factor(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    if (*pos < end_pos && tokens[*pos]->type == SQL_OPEN_PAREN) {
        (*pos)++; // Consume '('
        sql_ast_node_t *node = parse_arithmetic_expression(context, tokens, pos, end_pos);
        if (is_context_error(context))
            return NULL;
        if (*pos < end_pos && tokens[*pos]->type == SQL_CLOSE_PAREN) {
            (*pos)++; // Consume ')'
        } else {
            sql_ctx_error(context, "Expected closing parenthesis in arithmetic expression");
        }
        return node;
    }

    return parse_primary(context, tokens, pos, end_pos);
}

/* ------------------------------------------------------------------
 *  Primary, function calls, etc.
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

    // Handle parenthesized expressions (at the "primary" level)
    if (token->type == SQL_OPEN_PAREN) {
        (*pos)++; // Consume '('
        sql_ast_node_t *node = parse_expression(context, tokens, pos, end_pos);
        if (is_context_error(context))
            return NULL;
        if (*pos < end_pos && tokens[*pos]->type == SQL_CLOSE_PAREN) {
            (*pos)++; // Consume ')'
        } else {
            sql_ctx_error(context, "Expected closing parenthesis in parse_primary");
        }
        return node;
    }

    // Handle function calls
    if (token->type == SQL_FUNCTION) {
        (*pos)++; // Consume function name
        return parse_function_call(context, tokens, pos, end_pos);
    }

    // Handle identifiers, literals, or numbers
    if (token->type == SQL_IDENTIFIER ||
        token->type == SQL_COMPOUND_LITERAL ||
        token->type == SQL_LITERAL ||
        token->type == SQL_NUMBER) {
        sql_ast_node_t *node = create_ast_node(context, token);
        if (is_context_error(context))
            return NULL;
        (*pos)++;

        // Handle type-cast using '::'
        if (*pos < end_pos && tokens[*pos]->type == SQL_OPERATOR &&
            strcmp(tokens[*pos]->token, "::") == 0) {
            (*pos)++; // Consume '::'

            if (*pos < end_pos &&
                (tokens[*pos]->type == SQL_KEYWORD ||
                 tokens[*pos]->type == SQL_IDENTIFIER ||
                 tokens[*pos]->type == SQL_FUNCTION)) {
                sql_ast_node_t *cast_type = create_ast_node(context, tokens[*pos]);
                if (is_context_error(context))
                    return NULL;
                (*pos)++;

                // Create a CAST operator node
                sql_ast_node_t *cast_node = create_ast_node(context, &(sql_token_t){SQL_FUNCTION, "::"});
                if (is_context_error(context))
                    return NULL;
                cast_node->spec = sql_ctx_get_spec(context, "::");
                cast_node->left = node;      // Expression being cast
                cast_node->right = cast_type; // The type to cast to
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
    // func_name_token is the function name we consumed outside
    sql_token_t *func_name_token = tokens[*pos - 1];
    sql_ast_node_t *func_node = create_ast_node(context, func_name_token);
    if (is_context_error(context))
        return NULL;
    func_node->type = SQL_FUNCTION;

    if (*pos < end_pos && tokens[*pos]->type == SQL_OPEN_PAREN) {
        (*pos)++; // Consume '('

        // Handle function arguments
        sql_ast_node_t *arg_list_head = NULL;
        sql_ast_node_t *arg_list_tail = NULL;

        while (*pos < end_pos) {
            if (tokens[*pos]->type == SQL_CLOSE_PAREN) {
                (*pos)++; // Consume ')'
                break;
            }

            size_t arg_end = find_argument_end(context, tokens, *pos, end_pos, SQL_CLOSE_PAREN);
            if (is_context_error(context))
                return NULL;

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
                (*pos)++; // Consume ','
            }
        }

        func_node->left = arg_list_head; // Attach argument list
    } else {
        // If there's no '(' => treat as a literal function?
        func_node->type = SQL_FUNCTION_LITERAL;
        func_node->data_type = SQL_TYPE_STRING;
    }

    return func_node;
}

/* ------------------------------------------------------------------
 *  Comparison & special operators (BETWEEN, IN, etc.)
 * ------------------------------------------------------------------ */

static sql_ast_node_t *parse_between(sql_ctx_t *context, sql_ast_node_t *left,
                                     sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_ast_node_t *between_node = create_ast_node(context,
        &(sql_token_t){ .type = SQL_COMPARISON, .token = "BETWEEN" });
    if (is_context_error(context))
        return NULL;

    between_node->data_type = SQL_TYPE_BOOL;
    between_node->left = left; // The column being checked

    // Parse lower bound
    sql_ast_node_t *lower_bound = parse_arithmetic_expression(context, tokens, pos, end_pos);
    if (!lower_bound) {
        sql_ctx_error(context, "Expected lower bound after 'BETWEEN'");
        return NULL;
    }

    // Ensure "AND" is next
    if (*pos < end_pos && strcasecmp(tokens[*pos]->token, "AND") == 0) {
        (*pos)++; // Consume "AND"
    } else {
        sql_ctx_error(context, "Expected 'AND' in BETWEEN clause");
        return NULL;
    }

    // Parse upper bound
    sql_ast_node_t *upper_bound = parse_arithmetic_expression(context, tokens, pos, end_pos);
    if (!upper_bound) {
        sql_ctx_error(context, "Expected upper bound after 'AND' in BETWEEN");
        return NULL;
    }

    // Ensure AST structure is correct:
    sql_ast_node_t *bounds_node = create_ast_node(context, &(sql_token_t){ .type = SQL_TOKEN, .token = NULL });
    if (is_context_error(context))
        return NULL;

    bounds_node->left = lower_bound;
    bounds_node->right = upper_bound;

    between_node->right = bounds_node; // Attach bounds to BETWEEN node
    between_node->spec = sql_ctx_get_spec(context, "BETWEEN");

    return between_node;
}

static sql_ast_node_t *parse_not_between(sql_ctx_t *context, sql_ast_node_t *left,
                                         sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_ast_node_t *not_between_node = create_ast_node(context,
        &(sql_token_t){ .type = SQL_COMPARISON, .token = "NOT BETWEEN" });
    if (is_context_error(context))
        return NULL;
    not_between_node->data_type = SQL_TYPE_BOOL;
    not_between_node->left = left;

    sql_ast_node_t *lower_bound = parse_arithmetic_expression(context, tokens, pos, end_pos);
    if (!lower_bound) {
        sql_ctx_error(context, "Expected lower bound after 'NOT BETWEEN'");
        return NULL;
    }

    if (*pos < end_pos && strcasecmp(tokens[*pos]->token, "AND") == 0) {
        (*pos)++; // Consume 'AND'
    } else {
        sql_ctx_error(context, "Expected 'AND' in NOT BETWEEN clause");
        return NULL;
    }

    sql_ast_node_t *upper_bound = parse_arithmetic_expression(context, tokens, pos, end_pos);
    if (!upper_bound) {
        sql_ctx_error(context, "Expected upper bound after 'AND' in NOT BETWEEN");
        return NULL;
    }

    // Node to hold the two bounds
    sql_ast_node_t *bounds_node = create_ast_node(context, &(sql_token_t){ .type = SQL_TOKEN, .token = NULL });
    if (is_context_error(context))
        return NULL;
    bounds_node->left = lower_bound;
    bounds_node->right = upper_bound;
    not_between_node->right = bounds_node;
    not_between_node->spec = sql_ctx_get_spec(context, "NOT BETWEEN");
    if (is_context_error(context))
        return NULL;

    return not_between_node;
}

static sql_ast_node_t *parse_in_operator(sql_ctx_t *context, sql_ast_node_t *left,
                                         sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_ast_node_t *in_node = create_ast_node(context,
        &(sql_token_t){ .type = SQL_COMPARISON, .token = "IN" });
    if (is_context_error(context))
        return NULL;
    in_node->data_type = SQL_TYPE_BOOL;
    in_node->left = left;
    in_node->right = parse_in_list(context, tokens, pos, end_pos);
    if (is_context_error(context))
        return NULL;
    in_node->spec = sql_ctx_get_spec(context, "IN");
    if (is_context_error(context))
        return NULL;
    return in_node;
}

static sql_ast_node_t *parse_standard_comparison(sql_ctx_t *context,
                                                 sql_ast_node_t *left,
                                                 sql_token_t *operator_token,
                                                 sql_token_t **tokens,
                                                 size_t *pos,
                                                 size_t end_pos)
{
    // Create the comparison node (with original operator token)
    sql_ast_node_t *op_node = create_ast_node(context, operator_token);
    if (is_context_error(context))
        return NULL;
    op_node->data_type = SQL_TYPE_BOOL;

    // Special case: IS [NOT] NULL
    if (strcasecmp(operator_token->token, "IS") == 0) {
        if ((*pos + 1) < end_pos &&
            strcasecmp(tokens[*pos]->token, "NOT") == 0) {
            if(strcasecmp(tokens[*pos + 1]->token, "NULL") == 0) {
                (*pos) += 2; // consume "NOT NULL"
                op_node->value = aml_pool_strdup(context->pool, "IS NOT NULL");
                op_node->left = left;
                op_node->type = SQL_COMPARISON;
                op_node->spec = sql_ctx_get_spec(context, "IS NOT NULL");
                return op_node;
            } else if(strcasecmp(tokens[*pos + 1]->token, "FALSE") == 0) {
                (*pos) += 2; // consume "NOT FALSE"
                op_node->value = aml_pool_strdup(context->pool, "IS NOT FALSE");
                op_node->left = left;
                op_node->type = SQL_COMPARISON;
                op_node->spec = sql_ctx_get_spec(context, "IS NOT FALSE");
                return op_node;
            } else if(strcasecmp(tokens[*pos + 1]->token, "TRUE") == 0) {
                (*pos) += 2; // consume "NOT TRUE"
                op_node->value = aml_pool_strdup(context->pool, "IS NOT TRUE");
                op_node->left = left;
                op_node->type = SQL_COMPARISON;
                op_node->spec = sql_ctx_get_spec(context, "IS NOT TRUE");
                return op_node;
            } else {
                sql_ctx_error(context, "Invalid syntax after 'IS NOT'");
                return NULL;
            }
        } else if(*pos < end_pos) {
            if (strcasecmp(tokens[*pos]->token, "NULL") == 0) {
                (*pos)++; // consume "NULL"
                op_node->value = aml_pool_strdup(context->pool, "IS NULL");
                op_node->left = left;
                op_node->type = SQL_COMPARISON;
                op_node->spec = sql_ctx_get_spec(context, "IS NULL");
                return op_node;
            } else if(strcasecmp(tokens[*pos]->token, "FALSE") == 0) {
                (*pos)++; // consume "FALSE"
                op_node->value = aml_pool_strdup(context->pool, "IS FALSE");
                op_node->left = left;
                op_node->type = SQL_COMPARISON;
                op_node->spec = sql_ctx_get_spec(context, "IS FALSE");
                return op_node;
            } else if(strcasecmp(tokens[*pos]->token, "TRUE") == 0) {
                (*pos)++; // consume "TRUE"
                op_node->value = aml_pool_strdup(context->pool, "IS TRUE");
                op_node->left = left;
                op_node->type = SQL_COMPARISON;
                op_node->spec = sql_ctx_get_spec(context, "IS TRUE");
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

    // Otherwise parse right-hand side
    sql_ast_node_t *right = parse_arithmetic_expression(context, tokens, pos, end_pos);
    if (is_context_error(context))
        return NULL;

    /*
     *  If you want to store all comparisons in a single direction,
     *  e.g. rewriting A > B as B < A, you can restore your old “flip” logic:
     */
    if (operator_token->token[0] == '>') {
        // Flip '>' to '<' and swap left/right
        // (Equivalent to B < A)
        op_node->value[0] = '<';
        op_node->left = right;
        op_node->right = left;
        op_node->spec = sql_ctx_get_spec(context, op_node->value);
    }
    else {
        // Otherwise no flip
        op_node->left = left;
        op_node->right = right;
        op_node->spec = sql_ctx_get_spec(context, op_node->value);
    }

    return op_node;
}

/*
   The "NOT" handling for comparisons like `NOT BETWEEN`, `NOT IN`, etc.
   typically has to do with "keyword after NOT". This is triggered inside
   parse_comparison if we see a NOT before e.g. IN or BETWEEN.
*/
static sql_ast_node_t *parse_not_comparison_expression(sql_ctx_t *context,
                                                       sql_ast_node_t *left,
                                                       sql_token_t **tokens,
                                                       size_t *pos,
                                                       size_t end_pos)
{
    size_t not_pos = *pos;
    (*pos)++; // consume 'NOT'

    if (*pos < end_pos &&
        (tokens[*pos]->type == SQL_COMPARISON || tokens[*pos]->type == SQL_KEYWORD))
    {
        sql_token_t *operator_token = tokens[*pos];

        if (strcasecmp(operator_token->token, "BETWEEN") == 0) {
            (*pos)++; // consume 'BETWEEN'
            return parse_not_between(context, left, tokens, pos, end_pos);
        }
        else if (strcasecmp(operator_token->token, "LIKE") == 0)
        {
            // Merge "NOT" + operator into a single token
            char *combined_operator = aml_pool_strdupf(context->pool, "NOT %s", operator_token->token);

            sql_token_t not_operator_token = {
                .type = SQL_COMPARISON,
                .token = combined_operator
            };
            (*pos)++; // consume 'LIKE'

            sql_ast_node_t *not_node = create_ast_node(context, &not_operator_token);
            if (is_context_error(context))
                return NULL;
            not_node->data_type = SQL_TYPE_BOOL;
            not_node->left = left;

            // e.g. parse right side as arithmetic or ( ) ...
            not_node->right = parse_arithmetic_expression(context, tokens, pos, end_pos);
            not_node->spec = sql_ctx_get_spec(context, combined_operator);
            if (is_context_error(context))
                return NULL;

            return not_node;
        } else if (strcasecmp(operator_token->token, "IN") == 0) {
            (*pos)++; // consume 'IN'

            sql_ast_node_t *not_in_node = create_ast_node(context,
                &(sql_token_t){ .type = SQL_COMPARISON, .token = "NOT IN" });

            if (is_context_error(context)) return NULL;

            not_in_node->data_type = SQL_TYPE_BOOL;
            not_in_node->left = left;
            not_in_node->right = parse_in_list(context, tokens, pos, end_pos);
            if (is_context_error(context)) return NULL;

            not_in_node->spec = sql_ctx_get_spec(context, "NOT IN");
            if (!not_in_node->spec) {
                sql_ctx_error(context, "Missing function spec for NOT IN");
            }

            return not_in_node;
        }
    }

    // If none of the recognized "NOT" patterns matched, revert
    *pos = not_pos;
    return left;
}

/**
 * parse_comparison: parse something like
 *   <arithmetic> [NOT] [= | <> | BETWEEN ... | IN ... | IS ... ]
 */
sql_ast_node_t *parse_comparison(sql_ctx_t *context,
                                 sql_token_t **tokens,
                                 size_t *pos,
                                 size_t end_pos)
{
    // First parse the left-hand side as an arithmetic expression
    sql_ast_node_t *left = parse_arithmetic_expression(context, tokens, pos, end_pos);
    if (is_context_error(context))
        return NULL;

    // Check if there's a NOT or a comparison operator next
    if (*pos < end_pos) {
        // If the next token is NOT => might be "NOT BETWEEN", "NOT IN", etc.
        if (tokens[*pos]->type == SQL_NOT) {
            return parse_not_comparison_expression(context, left, tokens, pos, end_pos);
        }

        // If next token is a comparison operator or keyword => parse standard comparison
        if (tokens[*pos]->type == SQL_COMPARISON || tokens[*pos]->type == SQL_KEYWORD) {
            sql_token_t *operator_token = tokens[(*pos)++];
            // e.g. =, <, >, BETWEEN, IN, IS, ...
            if (strcasecmp(operator_token->token, "BETWEEN") == 0) {
                return parse_between(context, left, tokens, pos, end_pos);
            } else if (strcasecmp(operator_token->token, "IN") == 0) {
                return parse_in_operator(context, left, tokens, pos, end_pos);
            } else if (strcasecmp(operator_token->token, "IS") == 0) {
                return parse_standard_comparison(context, left, operator_token, tokens, pos, end_pos);
            } else {
                return parse_standard_comparison(context, left, operator_token, tokens, pos, end_pos);
            }
        }
    }

    // If no operator, just return the left side
    return left;
}

/* ------------------------------------------------------------------
 *  The new parse_unary function:
 *    - If we see NOT => parse another unary
 *    - If we see (  => parse full expression in parentheses
 *    - Otherwise => parse_comparison
 * ------------------------------------------------------------------ */

sql_ast_node_t *parse_unary(sql_ctx_t *context,
                            sql_token_t **tokens,
                            size_t *pos,
                            size_t end_pos)
{
    // Check for unary NOT
    if (*pos < end_pos && tokens[*pos]->type == SQL_NOT) {
        sql_token_t *not_token = tokens[(*pos)++];
        sql_ast_node_t *not_node = create_ast_node(context, not_token);
        if (is_context_error(context)) return NULL;

        // Recursively parse the next “unary” item
        not_node->left = parse_unary(context, tokens, pos, end_pos);
        return not_node;
    }

    // Check for parentheses => parse full sub-expression
    if (*pos < end_pos && tokens[*pos]->type == SQL_OPEN_PAREN) {
        (*pos)++; // consume '('
        sql_ast_node_t *expr = parse_expression(context, tokens, pos, end_pos);
        if (*pos < end_pos && tokens[*pos]->type == SQL_CLOSE_PAREN) {
            (*pos)++; // consume ')'
        } else {
            sql_ctx_error(context, "Expected closing parenthesis in parse_unary");
        }
        return expr;
    }

    // Otherwise, parse a comparison-level expression
    return parse_comparison(context, tokens, pos, end_pos);
}

/* ------------------------------------------------------------------
 *  AND/OR: parse_and_expression, parse_expression
 * ------------------------------------------------------------------ */

sql_ast_node_t *parse_and_expression(sql_ctx_t *context,
                                     sql_token_t **tokens,
                                     size_t *pos,
                                     size_t end_pos)
{
    // First parse a unary expression
    sql_ast_node_t *left = parse_unary(context, tokens, pos, end_pos);
    if (is_context_error(context))
        return NULL;

    while (*pos < end_pos) {
        if (tokens[*pos]->type == SQL_AND) {
            sql_token_t *token = tokens[(*pos)++];
            sql_ast_node_t *node = create_ast_node(context, token);
            if (is_context_error(context))
                return NULL;
            node->left = left;
            node->right = parse_unary(context, tokens, pos, end_pos);
            if (is_context_error(context))
                return NULL;
            node->data_type = SQL_TYPE_BOOL;
            left = node;
        }
        else if (tokens[*pos]->type == SQL_OR ||
                 tokens[*pos]->type == SQL_CLOSE_PAREN)
        {
            // Stop if we see OR or a closing parenthesis
            break;
        }
        else {
            // No more AND operators
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
    // Parse a chain of AND expressions
    sql_ast_node_t *left = parse_and_expression(context, tokens, pos, end_pos);
    if (is_context_error(context))
        return NULL;

    // Then handle OR operators at the top level
    while (*pos < end_pos) {
        if (tokens[*pos]->type == SQL_OR) {
            sql_token_t *token = tokens[(*pos)++];
            sql_ast_node_t *node = create_ast_node(context, token);
            if (is_context_error(context))
                return NULL;
            node->left = left;
            node->right = parse_and_expression(context, tokens, pos, end_pos);
            if (is_context_error(context))
                return NULL;
            node->data_type = SQL_TYPE_BOOL;
            left = node;
        }
        else if (tokens[*pos]->type == SQL_CLOSE_PAREN) {
            // If we’re inside parentheses, stop
            break;
        }
        else {
            break;
        }
    }

    return left;
}

/* ------------------------------------------------------------------
 *  parse_in_list: (A, B, C) or [A, B, C]
 * ------------------------------------------------------------------ */

sql_ast_node_t *parse_in_list(sql_ctx_t *context,
                              sql_token_t **tokens,
                              size_t *pos,
                              size_t token_count)
{
    sql_ast_node_t *list_node = (sql_ast_node_t *)aml_pool_alloc(context->pool, sizeof(sql_ast_node_t));
    list_node->type = SQL_LIST;
    list_node->value = NULL;
    list_node->left = NULL;
    list_node->right = NULL;
    list_node->next = NULL;

    // Expect '(' or '['
    if (*pos < token_count &&
        (tokens[*pos]->type == SQL_OPEN_BRACKET || tokens[*pos]->type == SQL_OPEN_PAREN))
    {
        sql_token_type_t closing_token_type =
            (tokens[*pos]->type == SQL_OPEN_BRACKET) ? SQL_CLOSE_BRACKET : SQL_CLOSE_PAREN;
        (*pos)++; // consume '[' or '('

        sql_ast_node_t *expr_list_head = NULL;
        sql_ast_node_t *expr_list_tail = NULL;

        while (*pos < token_count) {
            // Check if we've reached the closing token
            if (tokens[*pos]->type == closing_token_type) {
                (*pos)++; // consume ']' or ')'
                break;
            }

            size_t expr_end = find_argument_end(context, tokens, *pos, token_count, closing_token_type);
            if (is_context_error(context))
                return NULL;

            size_t expr_pos = *pos;
            sql_ast_node_t *expr = parse_expression(context, tokens, &expr_pos, expr_end);
            if (!expr) {
                sql_ctx_error(context, "Error parsing expression in IN list");
                break;
            }

            *pos = expr_pos;

            // Add to linked list
            if (!expr_list_head) {
                expr_list_head = expr;
                expr_list_tail = expr;
            } else {
                expr_list_tail->next = expr;
                expr_list_tail = expr;
            }

            // If next token is a comma, consume it
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

/* ------------------------------------------------------------------
 *  AST build for SELECT / FROM / WHERE at the top level
 * ------------------------------------------------------------------ */

sql_ast_node_t *build_ast(sql_ctx_t *context, sql_token_t **tokens, size_t token_count) {
    sql_ast_node_t *root = create_ast_node(context, &(sql_token_t){SQL_KEYWORD, "ROOT"});
    if (is_context_error(context))
        return NULL;
    size_t pos = 0;

    while (pos < token_count) {
        sql_token_t *token = tokens[pos];
        if (token->type == SQL_KEYWORD) {
            if (strcasecmp(token->token, "SELECT") == 0) {
                pos++;
                sql_ast_node_t *select_node = create_ast_node(context, token);
                if (is_context_error(context))
                    return NULL;
                // Parse columns until next keyword
                while (pos < token_count && tokens[pos]->type != SQL_KEYWORD) {
                    if (tokens[pos]->type == SQL_COMMA) {
                        pos++; // Skip comma
                        continue;
                    }
                    sql_ast_node_t *column_node = create_ast_node(context, tokens[pos++]);
                    if (is_context_error(context))
                        return NULL;
                    add_child_node(select_node, column_node);
                }
                add_child_node(root, select_node);
            }
            else if (strcasecmp(token->token, "FROM") == 0) {
                pos++;
                sql_ast_node_t *from_node = create_ast_node(context, token);
                if (is_context_error(context))
                    return NULL;
                // Parse tables until next keyword
                while (pos < token_count && tokens[pos]->type != SQL_KEYWORD) {
                    if (tokens[pos]->type == SQL_COMMA) {
                        pos++; // Skip comma
                        continue;
                    }
                    sql_ast_node_t *table_node = create_ast_node(context, tokens[pos++]);
                    if (is_context_error(context))
                        return NULL;
                    add_child_node(from_node, table_node);
                }
                add_child_node(root, from_node);
            }
            else if (strcasecmp(token->token, "WHERE") == 0) {
                pos++;
                sql_ast_node_t *where_node = create_ast_node(context, token);
                if (is_context_error(context))
                    return NULL;

                where_node->left = parse_expression(context, tokens, &pos, token_count);
                if (is_context_error(context))
                    return NULL;
                add_child_node(root, where_node);
            }
            else {
                // Other keywords?
                pos++;
            }
        } else {
            // Possibly a stray token
            pos++;
        }
    }

    return root;
}

/* ------------------------------------------------------------------
 *  Debug printing of the AST
 * ------------------------------------------------------------------ */

void print_ast(sql_ast_node_t *node, int depth) {
    if (!node) return;

    // Indent
    for (int i = 0; i < depth; i++) printf("  ");
    const char *type_name = sql_token_type_name(node->type);
    const char *data_type_name = sql_data_type_name(node->data_type);

    if (node->value) {
        printf("[%s] %s (DataType: %s) %p\n", type_name, node->value, data_type_name, node->spec);
    } else {
        printf("[%s] (DataType: %s) %p\n", type_name, data_type_name, node->spec);
    }

    // For BETWEEN / NOT BETWEEN
    if (node->type == SQL_COMPARISON &&
        (node->value &&
         (strcasecmp(node->value, "BETWEEN") == 0 ||
          strcasecmp(node->value, "NOT BETWEEN") == 0)))
    {
        // node->left is the expression
        for (int i = 0; i < depth + 1; i++) printf("  ");
        printf("Expression:\n");
        print_ast(node->left, depth + 2);

        // node->right is the bounds_node
        if (node->right) {
            sql_ast_node_t *bounds_node = node->right;

            // Lower Bound
            for (int i = 0; i < depth + 1; i++) printf("  ");
            printf("Lower Bound:\n");
            print_ast(bounds_node->left, depth + 2);

            // Upper Bound
            for (int i = 0; i < depth + 1; i++) printf("  ");
            printf("Upper Bound:\n");
            print_ast(bounds_node->right, depth + 2);
        }
    }
    // For IN / NOT IN operators
    else if (node->type == SQL_COMPARISON &&
             (node->value &&
              (strcasecmp(node->value, "IN") == 0 ||
               strcasecmp(node->value, "NOT IN") == 0)))
    {
        // node->left is the expression
        for (int i = 0; i < depth + 1; i++) printf("  ");
        printf("Expression:\n");
        print_ast(node->left, depth + 2);

        // node->right is the list of values
        if (node->right && node->right->left) {
            for (int i = 0; i < depth + 1; i++) printf("  ");
            printf("Values:\n");
            sql_ast_node_t *value_node = node->right->left;
            print_ast(value_node, depth + 2);
        }
    }
    // For binary operators
    else if (node->left && node->right) {
        for (int i = 0; i < depth + 1; i++) printf("  ");
        printf("Left:\n");
        print_ast(node->left, depth + 2);

        for (int i = 0; i < depth + 1; i++) printf("  ");
        printf("Right:\n");
        print_ast(node->right, depth + 2);
    }
    // Single child
    else if (node->left) {
        print_ast(node->left, depth + 1);
    }

    // Sibling nodes
    if (node->next) {
        print_ast(node->next, depth);
    }
}

sql_ast_node_t *find_clause(sql_ast_node_t *root, const char *clause_name) {
    if (!root) return NULL;

    if (root->type == SQL_KEYWORD && root->value &&
        strcasecmp(root->value, clause_name) == 0) {
        return root;
    }

    // Recursively search children
    if (root->left) {
        sql_ast_node_t *left_result = find_clause(root->left, clause_name);
        if (left_result) return left_result;
    }

    if (root->right) {
        sql_ast_node_t *right_result = find_clause(root->right, clause_name);
        if (right_result) return right_result;
    }

    if (root->next) {
        sql_ast_node_t *next_result = find_clause(root->next, clause_name);
        if (next_result) return next_result;
    }

    return NULL;
}
