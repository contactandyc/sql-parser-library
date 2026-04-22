// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_query.h"
#include "a-memory-library/aml_pool.h"
#include <strings.h>
#include <stdio.h>

// Parses: expr AS alias, expr2 AS alias2...
static sql_ast_node_t *parse_projection_list(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_ast_node_t *head = NULL;
    sql_ast_node_t *tail = NULL;

    while (*pos < end_pos && tokens[*pos]->type != SQL_KEYWORD) {
        sql_ast_node_t *expr = parse_expression(context, tokens, pos, end_pos);
        if (!expr) break;

        if (*pos < end_pos && tokens[*pos]->type == SQL_KEYWORD && strcasecmp(tokens[*pos]->token, "AS") == 0) {
            (*pos)++;
            if (*pos < end_pos && tokens[*pos]->type == SQL_IDENTIFIER) {
                expr->alias = aml_pool_strdup(context->pool, tokens[*pos]->token);
                (*pos)++;
            }
        } else if (*pos < end_pos && tokens[*pos]->type == SQL_IDENTIFIER) {
            expr->alias = aml_pool_strdup(context->pool, tokens[*pos]->token);
            (*pos)++;
        }

        if (!head) head = tail = expr;
        else {
            tail->next = expr;
            tail = expr;
        }

        if (*pos < end_pos && tokens[*pos]->type == SQL_COMMA) (*pos)++;
        else break;
    }
    return head;
}

// Parses a simple comma-separated list of expressions (Used for GROUP BY)
static sql_ast_node_t *parse_expression_list(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_ast_node_t *head = NULL;
    sql_ast_node_t *tail = NULL;

    while (*pos < end_pos && tokens[*pos]->type != SQL_KEYWORD && tokens[*pos]->type != SQL_SEMICOLON) {
        sql_ast_node_t *expr = parse_expression(context, tokens, pos, end_pos);
        if (!expr) break;

        if (!head) head = tail = expr;
        else {
            tail->next = expr;
            tail = expr;
        }

        if (*pos < end_pos && tokens[*pos]->type == SQL_COMMA) (*pos)++;
        else break;
    }
    return head;
}

// Parses: [INNER | LEFT | RIGHT | FULL] JOIN table [AS alias] ON expr
static sql_join_t *parse_join_list(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_join_t *head = NULL;
    sql_join_t *tail = NULL;

    while (*pos < end_pos) {
        sql_join_type_t jtype = JOIN_INNER;
        bool is_join = false;

        if (tokens[*pos]->type == SQL_KEYWORD) {
            if (strcasecmp(tokens[*pos]->token, "JOIN") == 0) {
                is_join = true;
                (*pos)++;
            } else if (strcasecmp(tokens[*pos]->token, "INNER") == 0) {
                jtype = JOIN_INNER;
                (*pos)++;
                if (*pos < end_pos && strcasecmp(tokens[*pos]->token, "JOIN") == 0) {
                    is_join = true;
                    (*pos)++;
                }
            } else if (strcasecmp(tokens[*pos]->token, "LEFT") == 0) {
                jtype = JOIN_LEFT;
                (*pos)++;
                if (*pos < end_pos && strcasecmp(tokens[*pos]->token, "OUTER") == 0) (*pos)++;
                if (*pos < end_pos && strcasecmp(tokens[*pos]->token, "JOIN") == 0) {
                    is_join = true;
                    (*pos)++;
                }
            } else if (strcasecmp(tokens[*pos]->token, "RIGHT") == 0) {
                jtype = JOIN_RIGHT;
                (*pos)++;
                if (*pos < end_pos && strcasecmp(tokens[*pos]->token, "OUTER") == 0) (*pos)++;
                if (*pos < end_pos && strcasecmp(tokens[*pos]->token, "JOIN") == 0) {
                    is_join = true;
                    (*pos)++;
                }
            } else if (strcasecmp(tokens[*pos]->token, "FULL") == 0) {
                jtype = JOIN_FULL;
                (*pos)++;
                if (*pos < end_pos && strcasecmp(tokens[*pos]->token, "OUTER") == 0) (*pos)++;
                if (*pos < end_pos && strcasecmp(tokens[*pos]->token, "JOIN") == 0) {
                    is_join = true;
                    (*pos)++;
                }
            }
        }

        if (!is_join) break;

        sql_join_t *node = aml_pool_zalloc(context->pool, sizeof(sql_join_t));
        node->type = jtype;

        if (*pos < end_pos && tokens[*pos]->type == SQL_IDENTIFIER) {
            node->table = aml_pool_strdup(context->pool, tokens[*pos]->token);
            (*pos)++;
        } else {
            sql_ctx_error(context, "Expected table name after JOIN");
            return head;
        }

        if (*pos < end_pos) {
            if (tokens[*pos]->type == SQL_KEYWORD && strcasecmp(tokens[*pos]->token, "AS") == 0) {
                (*pos)++;
                if (*pos < end_pos && tokens[*pos]->type == SQL_IDENTIFIER) {
                    node->alias = aml_pool_strdup(context->pool, tokens[*pos]->token);
                    (*pos)++;
                }
            } else if (tokens[*pos]->type == SQL_IDENTIFIER) {
                node->alias = aml_pool_strdup(context->pool, tokens[*pos]->token);
                (*pos)++;
            }
        }

        if (*pos < end_pos && tokens[*pos]->type == SQL_KEYWORD && strcasecmp(tokens[*pos]->token, "ON") == 0) {
            (*pos)++;
            node->on_condition = parse_expression(context, tokens, pos, end_pos);
        } else {
            sql_ctx_error(context, "Expected ON condition after JOIN");
            return head;
        }

        if (!head) head = tail = node;
        else {
            tail->next = node;
            tail = node;
        }
    }
    return head;
}

// Parses: col1 ASC, col2 DESC...
static sql_order_by_t *parse_order_by_list(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_order_by_t *head = NULL;
    sql_order_by_t *tail = NULL;

    while (*pos < end_pos && tokens[*pos]->type != SQL_SEMICOLON && tokens[*pos]->type != SQL_KEYWORD) {
        sql_order_by_t *node = aml_pool_zalloc(context->pool, sizeof(sql_order_by_t));

        node->expr = parse_expression(context, tokens, pos, end_pos);
        if (!node->expr) return NULL;
        node->is_desc = false;

        if (*pos < end_pos && tokens[*pos]->type == SQL_KEYWORD) {
            if (strcasecmp(tokens[*pos]->token, "DESC") == 0) {
                node->is_desc = true;
                (*pos)++;
            } else if (strcasecmp(tokens[*pos]->token, "ASC") == 0) {
                (*pos)++;
            }
        }

        if (!head) head = tail = node;
        else {
            tail->next = node;
            tail = node;
        }

        if (*pos < end_pos && tokens[*pos]->type == SQL_COMMA) (*pos)++;
        else break;
    }
    return head;
}

// The Top-Level Parser
sql_select_t *sql_parse_query(sql_ctx_t *context, sql_token_t **tokens, size_t token_count) {
    if (token_count == 0) return NULL;
    size_t pos = 0;

    if (tokens[pos]->type != SQL_KEYWORD || strcasecmp(tokens[pos]->token, "SELECT") != 0) {
        sql_ctx_error(context, "Query must start with SELECT");
        return NULL;
    }
    pos++;

    sql_select_t *query = aml_pool_zalloc(context->pool, sizeof(sql_select_t));

    // 1. Projections
    if (pos < token_count && tokens[pos]->type == SQL_OPERATOR && strcmp(tokens[pos]->token, "*") == 0) {
        query->is_star = true;
        pos++;
    } else {
        query->is_star = false;
        query->columns = parse_projection_list(context, tokens, &pos, token_count);
    }

    // 2. FROM Clause
    if (pos < token_count && tokens[pos]->type == SQL_KEYWORD && strcasecmp(tokens[pos]->token, "FROM") == 0) {
        pos++;
        if (pos < token_count && tokens[pos]->type == SQL_IDENTIFIER) {
            query->table = aml_pool_strdup(context->pool, tokens[pos]->token);
            pos++;

            if (pos < token_count) {
                if (tokens[pos]->type == SQL_KEYWORD && strcasecmp(tokens[pos]->token, "AS") == 0) {
                    pos++;
                    if (pos < token_count && tokens[pos]->type == SQL_IDENTIFIER) {
                        query->table_alias = aml_pool_strdup(context->pool, tokens[pos]->token);
                        pos++;
                    }
                } else if (tokens[pos]->type == SQL_IDENTIFIER) {
                    query->table_alias = aml_pool_strdup(context->pool, tokens[pos]->token);
                    pos++;
                }
            }
        } else {
            sql_ctx_error(context, "Expected table name after FROM");
            return NULL;
        }
    }

    // 3. JOIN Clauses
    query->joins = parse_join_list(context, tokens, &pos, token_count);

    // 4. WHERE Clause
    if (pos < token_count && tokens[pos]->type == SQL_KEYWORD && strcasecmp(tokens[pos]->token, "WHERE") == 0) {
        pos++;
        query->where_clause = parse_expression(context, tokens, &pos, token_count);
    }

    // 5. GROUP BY Clause
    if (pos < token_count && tokens[pos]->type == SQL_KEYWORD && strcasecmp(tokens[pos]->token, "GROUP") == 0) {
        pos++;
        if (pos < token_count && tokens[pos]->type == SQL_KEYWORD && strcasecmp(tokens[pos]->token, "BY") == 0) {
            pos++;
            query->group_by = parse_expression_list(context, tokens, &pos, token_count);
        } else {
            sql_ctx_error(context, "Expected BY after GROUP");
            return NULL;
        }
    }

    // 6. HAVING Clause
    if (pos < token_count && tokens[pos]->type == SQL_KEYWORD && strcasecmp(tokens[pos]->token, "HAVING") == 0) {
        pos++;
        query->having_clause = parse_expression(context, tokens, &pos, token_count);
    }

    // 7. ORDER BY Clause
    if (pos < token_count && tokens[pos]->type == SQL_KEYWORD && strcasecmp(tokens[pos]->token, "ORDER") == 0) {
        pos++;
        if (pos < token_count && tokens[pos]->type == SQL_KEYWORD && strcasecmp(tokens[pos]->token, "BY") == 0) {
            pos++;
            query->order_by = parse_order_by_list(context, tokens, &pos, token_count);
        } else {
            sql_ctx_error(context, "Expected BY after ORDER");
            return NULL;
        }
    }

    // 8. LIMIT Clause
    if (pos < token_count && tokens[pos]->type == SQL_KEYWORD && strcasecmp(tokens[pos]->token, "LIMIT") == 0) {
        pos++;
        // Because it's an expression, you can parse "LIMIT 5 * page_size"
        query->limit = parse_expression(context, tokens, &pos, token_count);
    }

    // 9. OFFSET Clause
    if (pos < token_count && tokens[pos]->type == SQL_KEYWORD && strcasecmp(tokens[pos]->token, "OFFSET") == 0) {
        pos++;
        // Because it's an expression, you can parse "OFFSET 100 + 20"
        query->offset = parse_expression(context, tokens, &pos, token_count);
    }

    if (pos < token_count && tokens[pos]->type != SQL_SEMICOLON) {
        sql_ctx_error(context, "Unexpected token at end of query: %s", tokens[pos]->token);
        return NULL;
    }

    return query;
}

void sql_print_query(sql_select_t *query, int depth) {
    if (!query) return;

    printf("--- SQL QUERY AST ---\n");
    if (query->table) {
        printf("FROM TABLE: %s", query->table);
        if (query->table_alias) printf(" AS %s", query->table_alias);
        printf("\n");
    }

    if (query->joins) {
        sql_join_t *j = query->joins;
        while (j) {
            const char *type_str = j->type == JOIN_LEFT ? "LEFT" :
                                   j->type == JOIN_RIGHT ? "RIGHT" :
                                   j->type == JOIN_FULL ? "FULL" : "INNER";
            printf("  %s JOIN %s", type_str, j->table);
            if (j->alias) printf(" AS %s", j->alias);
            printf("\n    ON:\n");
            print_ast(j->on_condition, 3);
            j = j->next;
        }
    }

    printf("PROJECTIONS:\n");
    if (query->is_star) {
        printf("  * (All Columns)\n");
    } else {
        sql_ast_node_t *col = query->columns;
        while (col) {
            printf("  [Expression] ALIAS: %s\n", col->alias ? col->alias : "(none)");
            print_ast(col, 2);
            col = col->next;
        }
    }

    if (query->where_clause) {
        printf("WHERE:\n");
        print_ast(query->where_clause, 1);
    }

    if (query->group_by) {
        printf("GROUP BY:\n");
        sql_ast_node_t *gb = query->group_by;
        while (gb) {
            print_ast(gb, 1);
            gb = gb->next;
        }
    }

    if (query->having_clause) {
        printf("HAVING:\n");
        print_ast(query->having_clause, 1);
    }

    if (query->order_by) {
        printf("ORDER BY:\n");
        sql_order_by_t *ob = query->order_by;
        while (ob) {
            printf("  [Expression] DIR: %s\n", ob->is_desc ? "DESC" : "ASC");
            print_ast(ob->expr, 2);
            ob = ob->next;
        }
    }

    if (query->limit) {
        printf("LIMIT:\n");
        print_ast(query->limit, 1);
    }

    if (query->offset) {
        printf("OFFSET:\n");
        print_ast(query->offset, 1);
    }
    printf("---------------------\n");
}


// ------------------------------------------------------------------
// AST to String Generator (Unparser)
// ------------------------------------------------------------------

// Forward declaration for recursion
static char *ast_to_string(sql_ctx_t *ctx, sql_ast_node_t *node);

// Helper to concatenate a linked list of expressions (e.g. function args, group by)
static char *ast_list_to_string(sql_ctx_t *ctx, sql_ast_node_t *head) {
    char *result = aml_pool_strdup(ctx->pool, "");
    sql_ast_node_t *current = head;
    while (current) {
        char *expr_str = ast_to_string(ctx, current);
        if (strlen(result) > 0) {
            result = aml_pool_strdupf(ctx->pool, "%s, %s", result, expr_str);
        } else {
            result = expr_str;
        }
        current = current->next;
    }
    return result;
}

// Inside sql_query.c -> ast_to_string()
static char *ast_to_string(sql_ctx_t *ctx, sql_ast_node_t *node) {
    if (!node) return aml_pool_strdup(ctx->pool, "");

    switch (node->type) {
        case SQL_IDENTIFIER:
        case SQL_NUMBER:
        case SQL_COMPOUND_LITERAL:
            if (strncasecmp(node->value, "TIMESTAMP ", 10) == 0 && node->value[10] != '\'') {
                return aml_pool_strdupf(ctx->pool, "TIMESTAMP '%s'", node->value + 10);
            }
            return aml_pool_strdup(ctx->pool, node->value);
        case SQL_KEYWORD: // <--- CRITICAL FIX: Ensure keywords (like LIMIT) stringify correctly if captured as expressions
            return aml_pool_strdup(ctx->pool, node->value);

        case SQL_NULL:
            return aml_pool_strdup(ctx->pool, "NULL");

        case SQL_LITERAL:
            if (node->data_type == SQL_TYPE_BOOL) {
                return aml_pool_strdup(ctx->pool, node->value);
            }
            return aml_pool_strdupf(ctx->pool, "'%s'", node->value);

        case SQL_FUNCTION_LITERAL:
            return aml_pool_strdup(ctx->pool, node->value);

        case SQL_NOT: {
            char *inner = ast_to_string(ctx, node->left);
            return aml_pool_strdupf(ctx->pool, "(NOT %s)", inner);
        }

        case SQL_AND:
        case SQL_OR:
        case SQL_OPERATOR:
        case SQL_COMPARISON: {
            if (strcasecmp(node->value, "BETWEEN") == 0 || strcasecmp(node->value, "NOT BETWEEN") == 0) {
                char *left = ast_to_string(ctx, node->left);
                char *lower = ast_to_string(ctx, node->right->left);
                char *upper = ast_to_string(ctx, node->right->right);
                return aml_pool_strdupf(ctx->pool, "(%s %s %s AND %s)", left, node->value, lower, upper);
            }

            if (strncasecmp(node->value, "IS", 2) == 0) {
                char *left_str = ast_to_string(ctx, node->left);

                if (node->right) {
                    // It's a binary 'IS' operator (IS [NOT] DISTINCT FROM)
                    char *right_str = ast_to_string(ctx, node->right);
                    return aml_pool_strdupf(ctx->pool, "(%s %s %s)", left_str, node->value, right_str);
                } else {
                    // It's a unary 'IS' operator (IS NULL, IS TRUE)
                    return aml_pool_strdupf(ctx->pool, "(%s %s)", left_str, node->value);
                }
            }

            if (strcasecmp(node->value, "IN") == 0 || strcasecmp(node->value, "NOT IN") == 0) {
                char *left = ast_to_string(ctx, node->left);
                char *list = ast_to_string(ctx, node->right);
                return aml_pool_strdupf(ctx->pool, "(%s %s %s)", left, node->value, list);
            }

            if (node->left && node->right) {
                char *left = ast_to_string(ctx, node->left);
                char *right = ast_to_string(ctx, node->right);
                return aml_pool_strdupf(ctx->pool, "(%s %s %s)", left, node->value, right);
            }

            if (node->left) {
                return aml_pool_strdupf(ctx->pool, "%s%s", node->value, ast_to_string(ctx, node->left));
            }
            break;
        }

        case SQL_FUNCTION: {
            if (strcasecmp(node->value, "CASE") == 0) {
                char *res = aml_pool_strdup(ctx->pool, "CASE");
                sql_ast_node_t *curr = node->left;
                while (curr) {
                    if (strcasecmp(curr->value, "WHEN") == 0) {
                        char *cond = ast_to_string(ctx, curr->left);
                        char *val = ast_to_string(ctx, curr->right);
                        res = aml_pool_strdupf(ctx->pool, "%s WHEN %s THEN %s", res, cond, val);
                    } else if (strcasecmp(curr->value, "ELSE") == 0) {
                        char *val = ast_to_string(ctx, curr->left);
                        res = aml_pool_strdupf(ctx->pool, "%s ELSE %s", res, val);
                    }
                    curr = curr->next;
                }
                return aml_pool_strdupf(ctx->pool, "%s END", res);
            }
            if (node->left && node->left->type == SQL_KEYWORD && strcasecmp(node->left->value, "FROM") == 0) {
                char *field = node->left->left->value;
                char *source = ast_to_string(ctx, node->left->right);
                return aml_pool_strdupf(ctx->pool, "EXTRACT(%s FROM %s)", field, source);
            }
            if (strcasecmp(node->value, "POSITION") == 0 && node->left && node->left->next) {
                char *substr = ast_to_string(ctx, node->left);
                char *str = ast_to_string(ctx, node->left->next);
                return aml_pool_strdupf(ctx->pool, "POSITION(%s IN %s)", substr, str);
            }
            if (strcasecmp(node->value, "CAST") == 0 && node->left && node->left->next && node->left->next->next) {
                char *expr = ast_to_string(ctx, node->left);
                char *type_str = ast_to_string(ctx, node->left->next->next);
                return aml_pool_strdupf(ctx->pool, "CAST(%s AS %s)", expr, type_str);
            }
            char *args = ast_list_to_string(ctx, node->left);
            return aml_pool_strdupf(ctx->pool, "%s(%s)", node->value, args);
        }

        case SQL_LIST: {
            char *args = ast_list_to_string(ctx, node->left);
            return aml_pool_strdupf(ctx->pool, "(%s)", args);
        }

        default:
            break;
    }
    return aml_pool_strdup(ctx->pool, "");
}

// ------------------------------------------------------------------
// Main Query Unparser
// ------------------------------------------------------------------

char *sql_query_to_string(sql_ctx_t *ctx, sql_select_t *query) {
    if (!query) return NULL;

    char *sql = aml_pool_strdup(ctx->pool, "SELECT ");

    // 1. Projections
    if (query->is_star) {
        sql = aml_pool_strdupf(ctx->pool, "%s*", sql);
    } else {
        sql_ast_node_t *col = query->columns;
        while (col) {
            char *expr_str = ast_to_string(ctx, col);
            if (col->alias) {
                sql = aml_pool_strdupf(ctx->pool, "%s%s AS %s", sql, expr_str, col->alias);
            } else {
                sql = aml_pool_strdupf(ctx->pool, "%s%s", sql, expr_str);
            }

            col = col->next;
            if (col) sql = aml_pool_strdupf(ctx->pool, "%s, ", sql);
        }
    }

    // 2. FROM
    if (query->table) {
        sql = aml_pool_strdupf(ctx->pool, "%s FROM %s", sql, query->table);
        if (query->table_alias) {
            sql = aml_pool_strdupf(ctx->pool, "%s AS %s", sql, query->table_alias);
        }
    }

    // 3. JOINs
    if (query->joins) {
        sql_join_t *j = query->joins;
        while (j) {
            const char *type_str = j->type == JOIN_LEFT ? "LEFT" :
                                   j->type == JOIN_RIGHT ? "RIGHT" :
                                   j->type == JOIN_FULL ? "FULL OUTER" : "INNER";

            sql = aml_pool_strdupf(ctx->pool, "%s %s JOIN %s", sql, type_str, j->table);
            if (j->alias) {
                sql = aml_pool_strdupf(ctx->pool, "%s AS %s", sql, j->alias);
            }

            char *on_str = ast_to_string(ctx, j->on_condition);
            sql = aml_pool_strdupf(ctx->pool, "%s ON %s", sql, on_str);

            j = j->next;
        }
    }

    // 4. WHERE
    if (query->where_clause) {
        char *where_str = ast_to_string(ctx, query->where_clause);
        sql = aml_pool_strdupf(ctx->pool, "%s WHERE %s", sql, where_str);
    }

    // 5. GROUP BY
    if (query->group_by) {
        char *gb_str = ast_list_to_string(ctx, query->group_by);
        sql = aml_pool_strdupf(ctx->pool, "%s GROUP BY %s", sql, gb_str);
    }

    // 6. HAVING
    if (query->having_clause) {
        char *having_str = ast_to_string(ctx, query->having_clause);
        sql = aml_pool_strdupf(ctx->pool, "%s HAVING %s", sql, having_str);
    }

    // 7. ORDER BY
    if (query->order_by) {
        sql = aml_pool_strdupf(ctx->pool, "%s ORDER BY ", sql);
        sql_order_by_t *ob = query->order_by;
        while (ob) {
            char *expr_str = ast_to_string(ctx, ob->expr);
            sql = aml_pool_strdupf(ctx->pool, "%s%s %s", sql, expr_str, ob->is_desc ? "DESC" : "ASC");

            ob = ob->next;
            if (ob) sql = aml_pool_strdupf(ctx->pool, "%s, ", sql);
        }
    }

    // 8. LIMIT
    if (query->limit) {
        char *limit_str = ast_to_string(ctx, query->limit);
        sql = aml_pool_strdupf(ctx->pool, "%s LIMIT %s", sql, limit_str);
    }

    // 9. OFFSET
    if (query->offset) {
        char *offset_str = ast_to_string(ctx, query->offset);
        sql = aml_pool_strdupf(ctx->pool, "%s OFFSET %s", sql, offset_str);
    }

    return sql;
}
