// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_query.h"
#include "a-memory-library/aml_pool.h"
#include <strings.h>
#include <stdio.h>

static inline bool is_context_error(sql_ctx_t *context) {
    return context->errors != NULL;
}

// Ensure the prototype is available internally
sql_select_t *sql_parse_select_query(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t token_count);

static sql_ast_node_t *parse_projection_list(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_ast_node_t *head = NULL;
    sql_ast_node_t *tail = NULL;

    while (*pos < end_pos && tokens[*pos]->type != SQL_KEYWORD) {
        sql_ast_node_t *expr = sql_parse_expression(context, tokens, pos, end_pos);
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

static sql_ast_node_t *sql_parse_expression_list(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_ast_node_t *head = NULL;
    sql_ast_node_t *tail = NULL;

    while (*pos < end_pos && tokens[*pos]->type != SQL_KEYWORD && tokens[*pos]->type != SQL_SEMICOLON) {
        sql_ast_node_t *expr = sql_parse_expression(context, tokens, pos, end_pos);
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

        if (*pos < end_pos && tokens[*pos]->type == SQL_OPEN_PAREN) {
            (*pos)++; // Consume '('
            node->subquery = sql_parse_select_query(context, tokens, pos, end_pos);

            if (*pos < end_pos && tokens[*pos]->type == SQL_CLOSE_PAREN) {
                (*pos)++; // Consume ')'
            } else {
                sql_ctx_error(context, "Expected closing parenthesis after JOIN subquery");
                return head;
            }
        } else if (*pos < end_pos && tokens[*pos]->type == SQL_IDENTIFIER) {
            node->table = aml_pool_strdup(context->pool, tokens[*pos]->token);
            (*pos)++;
        } else {
            sql_ctx_error(context, "Expected table name or subquery after JOIN");
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
            node->on_condition = sql_parse_expression(context, tokens, pos, end_pos);
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

static sql_order_by_t *parse_order_by_list(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t end_pos) {
    sql_order_by_t *head = NULL;
    sql_order_by_t *tail = NULL;

    while (*pos < end_pos && tokens[*pos]->type != SQL_SEMICOLON && tokens[*pos]->type != SQL_KEYWORD) {
        sql_order_by_t *node = aml_pool_zalloc(context->pool, sizeof(sql_order_by_t));

        node->expr = sql_parse_expression(context, tokens, pos, end_pos);
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

sql_select_t *sql_parse_select_query(sql_ctx_t *context, sql_token_t **tokens, size_t *pos, size_t token_count) {
    if (*pos >= token_count) return NULL;

    sql_select_t *query = aml_pool_zalloc(context->pool, sizeof(sql_select_t));

    if (tokens[*pos]->type == SQL_KEYWORD && strcasecmp(tokens[*pos]->token, "EXPLAIN") == 0) {
        query->is_explain = true;
        (*pos)++;
    }

    if (*pos < token_count && tokens[*pos]->type == SQL_KEYWORD && strcasecmp(tokens[*pos]->token, "WITH") == 0) {
        (*pos)++;
        sql_cte_t *cte_head = NULL, *cte_tail = NULL;

        while (*pos < token_count) {
            sql_cte_t *cte = aml_pool_zalloc(context->pool, sizeof(sql_cte_t));

            if (tokens[*pos]->type == SQL_IDENTIFIER) {
                cte->alias = aml_pool_strdup(context->pool, tokens[*pos]->token);
                (*pos)++;
            } else {
                sql_ctx_error(context, "Expected identifier after WITH");
                return NULL;
            }

            if (*pos < token_count && tokens[*pos]->type == SQL_KEYWORD && strcasecmp(tokens[*pos]->token, "AS") == 0) {
                (*pos)++;
            } else {
                sql_ctx_error(context, "Expected AS after CTE alias");
                return NULL;
            }

            if (*pos < token_count && tokens[*pos]->type == SQL_OPEN_PAREN) {
                (*pos)++;
                cte->query = sql_parse_select_query(context, tokens, pos, token_count);

                if (*pos < token_count && tokens[*pos]->type == SQL_CLOSE_PAREN) {
                    (*pos)++;
                } else {
                    sql_ctx_error(context, "Expected closing parenthesis after CTE query");
                    return NULL;
                }
            } else {
                sql_ctx_error(context, "Expected '(' after CTE AS");
                return NULL;
            }

            if (!cte_head) cte_head = cte_tail = cte;
            else { cte_tail->next = cte; cte_tail = cte; }

            if (*pos < token_count && tokens[*pos]->type == SQL_COMMA) {
                (*pos)++;
            } else {
                break;
            }
        }
        query->ctes = cte_head;
    }

    if (*pos >= token_count || tokens[*pos]->type != SQL_KEYWORD || strcasecmp(tokens[*pos]->token, "SELECT") != 0) {
        sql_ctx_error(context, "Query must start with SELECT (or EXPLAIN/WITH)");
        return NULL;
    }
    (*pos)++;

    if (*pos < token_count && tokens[*pos]->type == SQL_OPERATOR && strcmp(tokens[*pos]->token, "*") == 0) {
        query->is_star = true;
        (*pos)++;
    } else {
        query->is_star = false;
        query->columns = parse_projection_list(context, tokens, pos, token_count);
    }

    if (*pos < token_count && tokens[*pos]->type == SQL_KEYWORD && strcasecmp(tokens[*pos]->token, "FROM") == 0) {
        (*pos)++;

        if (*pos < token_count && tokens[*pos]->type == SQL_OPEN_PAREN) {
            (*pos)++;
            query->subquery = sql_parse_select_query(context, tokens, pos, token_count);

            if (*pos < token_count && tokens[*pos]->type == SQL_CLOSE_PAREN) {
                (*pos)++;
            } else {
                sql_ctx_error(context, "Expected closing parenthesis after FROM subquery");
                return NULL;
            }
        } else if (*pos < token_count && tokens[*pos]->type == SQL_IDENTIFIER) {
            query->table = aml_pool_strdup(context->pool, tokens[*pos]->token);
            (*pos)++;
        } else {
            sql_ctx_error(context, "Expected table name or subquery after FROM");
            return NULL;
        }

        if (*pos < token_count) {
            if (tokens[*pos]->type == SQL_KEYWORD && strcasecmp(tokens[*pos]->token, "AS") == 0) {
                (*pos)++;
                if (*pos < token_count && tokens[*pos]->type == SQL_IDENTIFIER) {
                    query->table_alias = aml_pool_strdup(context->pool, tokens[*pos]->token);
                    (*pos)++;
                }
            } else if (tokens[*pos]->type == SQL_IDENTIFIER) {
                query->table_alias = aml_pool_strdup(context->pool, tokens[*pos]->token);
                (*pos)++;
            }
        }
    }

    query->joins = parse_join_list(context, tokens, pos, token_count);

    if (*pos < token_count && tokens[*pos]->type == SQL_KEYWORD && strcasecmp(tokens[*pos]->token, "WHERE") == 0) {
        (*pos)++;
        query->where_clause = sql_parse_expression(context, tokens, pos, token_count);
    }

    if (*pos < token_count && tokens[*pos]->type == SQL_KEYWORD && strcasecmp(tokens[*pos]->token, "GROUP") == 0) {
        (*pos)++;
        if (*pos < token_count && tokens[*pos]->type == SQL_KEYWORD && strcasecmp(tokens[*pos]->token, "BY") == 0) {
            (*pos)++;
            query->group_by = sql_parse_expression_list(context, tokens, pos, token_count);
        } else {
            sql_ctx_error(context, "Expected BY after GROUP");
            return NULL;
        }
    }

    if (*pos < token_count && tokens[*pos]->type == SQL_KEYWORD && strcasecmp(tokens[*pos]->token, "HAVING") == 0) {
        (*pos)++;
        query->having_clause = sql_parse_expression(context, tokens, pos, token_count);
    }

    if (*pos < token_count && tokens[*pos]->type == SQL_KEYWORD && strcasecmp(tokens[*pos]->token, "ORDER") == 0) {
        (*pos)++;
        if (*pos < token_count && tokens[*pos]->type == SQL_KEYWORD && strcasecmp(tokens[*pos]->token, "BY") == 0) {
            (*pos)++;
            query->order_by = parse_order_by_list(context, tokens, pos, token_count);
        } else {
            sql_ctx_error(context, "Expected BY after ORDER");
            return NULL;
        }
    }

    if (*pos < token_count && tokens[*pos]->type == SQL_KEYWORD && strcasecmp(tokens[*pos]->token, "LIMIT") == 0) {
        (*pos)++;
        query->limit = sql_parse_expression(context, tokens, pos, token_count);
    }

    if (*pos < token_count && tokens[*pos]->type == SQL_KEYWORD && strcasecmp(tokens[*pos]->token, "OFFSET") == 0) {
        (*pos)++;
        query->offset = sql_parse_expression(context, tokens, pos, token_count);
    }

    return query;
}

sql_select_t *sql_parse_query(sql_ctx_t *context, sql_token_t **tokens, size_t token_count) {
    if (token_count == 0) return NULL;
    size_t pos = 0;

    sql_select_t *query = sql_parse_select_query(context, tokens, &pos, token_count);

    if (is_context_error(context)) return NULL;

    if (pos < token_count && tokens[pos]->type != SQL_SEMICOLON) {
        sql_ctx_error(context, "Unexpected token at end of query: %s", tokens[pos]->token);
        return NULL;
    }

    return query;
}

// --- PRINT & TO_STRING ---
void sql_print_query(sql_select_t *query, int depth) {
    if (!query) return;

    for(int i=0; i<depth; i++) printf("  ");
    printf("--- SQL QUERY AST ---\n");
    if (query->is_explain) {
        for(int i=0; i<depth; i++) printf("  ");
        printf("[EXPLAIN MODE ENABLED]\n");
    }

    if (query->ctes) {
        for(int i=0; i<depth; i++) printf("  ");
        printf("WITH CTEs:\n");
        sql_cte_t *cte = query->ctes;
        while(cte) {
            for(int i=0; i<depth+1; i++) printf("  ");
            printf("%s AS:\n", cte->alias);
            sql_print_query(cte->query, depth + 2);
            cte = cte->next;
        }
    }

    if (query->subquery) {
        for(int i=0; i<depth; i++) printf("  ");
        printf("FROM SUBQUERY:\n");
        sql_print_query(query->subquery, depth + 1);
        if (query->table_alias) {
            for(int i=0; i<depth; i++) printf("  ");
            printf("AS %s\n", query->table_alias);
        }
    } else if (query->table) {
        for(int i=0; i<depth; i++) printf("  ");
        printf("FROM TABLE: %s", query->table);
        if (query->table_alias) printf(" AS %s", query->table_alias);
        printf("\n");
    }

    if (query->joins) {
        sql_join_t *j = query->joins;
        while (j) {
            for(int i=0; i<depth; i++) printf("  ");
            const char *type_str = j->type == JOIN_LEFT ? "LEFT" :
                                   j->type == JOIN_RIGHT ? "RIGHT" :
                                   j->type == JOIN_FULL ? "FULL" : "INNER";

            if (j->subquery) {
                printf("%s JOIN SUBQUERY:\n", type_str);
                sql_print_query(j->subquery, depth + 1);
                if (j->alias) {
                    for(int i=0; i<depth; i++) printf("  ");
                    printf("AS %s\n", j->alias);
                }
            } else {
                printf("%s JOIN %s", type_str, j->table);
                if (j->alias) printf(" AS %s", j->alias);
                printf("\n");
            }

            for(int i=0; i<depth; i++) printf("  ");
            printf("  ON:\n");
            sql_print_ast(j->on_condition, depth + 2);
            j = j->next;
        }
    }

    for(int i=0; i<depth; i++) printf("  ");
    printf("PROJECTIONS:\n");
    if (query->is_star) {
        for(int i=0; i<depth; i++) printf("  ");
        printf("  * (All Columns)\n");
    } else {
        sql_ast_node_t *col = query->columns;
        while (col) {
            for(int i=0; i<depth; i++) printf("  ");
            printf("  [Expression] ALIAS: %s\n", col->alias ? col->alias : "(none)");
            sql_print_ast(col, depth + 2);
            col = col->next;
        }
    }
}

static char *ast_list_to_string(sql_ctx_t *ctx, sql_ast_node_t *head) {
    char *result = aml_pool_strdup(ctx->pool, "");
    sql_ast_node_t *current = head;
    while (current) {
        char *expr_str = sql_ast_to_string(ctx, current);
        if (strlen(result) > 0) {
            result = aml_pool_strdupf(ctx->pool, "%s, %s", result, expr_str);
        } else {
            result = expr_str;
        }
        current = current->next;
    }
    return result;
}

char *sql_ast_to_string(sql_ctx_t *ctx, sql_ast_node_t *node) {
    if (!node) return aml_pool_strdup(ctx->pool, "");

    switch (node->type) {
        case SQL_IDENTIFIER:
        case SQL_NUMBER:
        case SQL_COMPOUND_LITERAL:
            if (strncasecmp(node->value, "TIMESTAMP ", 10) == 0 && node->value[10] != '\'') {
                return aml_pool_strdupf(ctx->pool, "TIMESTAMP '%s'", node->value + 10);
            }
            return aml_pool_strdup(ctx->pool, node->value);
        case SQL_KEYWORD:
            return aml_pool_strdup(ctx->pool, node->value);
        case SQL_NULL: return aml_pool_strdup(ctx->pool, "NULL");
        case SQL_LITERAL:
            if (node->data_type == SQL_TYPE_BOOL) return aml_pool_strdup(ctx->pool, node->value);
            return aml_pool_strdupf(ctx->pool, "'%s'", node->value);
        case SQL_FUNCTION_LITERAL: return aml_pool_strdup(ctx->pool, node->value);
        case SQL_NOT: {
            char *inner = sql_ast_to_string(ctx, node->left);
            return aml_pool_strdupf(ctx->pool, "(NOT %s)", inner);
        }
        case SQL_AND:
        case SQL_OR:
        case SQL_OPERATOR:
        case SQL_COMPARISON: {
            if (strcasecmp(node->value, "BETWEEN") == 0 || strcasecmp(node->value, "NOT BETWEEN") == 0) {
                char *left = sql_ast_to_string(ctx, node->left);
                char *lower = sql_ast_to_string(ctx, node->right->left);
                char *upper = sql_ast_to_string(ctx, node->right->right);
                return aml_pool_strdupf(ctx->pool, "(%s %s %s AND %s)", left, node->value, lower, upper);
            }
            if (strncasecmp(node->value, "IS", 2) == 0) {
                char *left_str = sql_ast_to_string(ctx, node->left);
                if (node->right) {
                    char *right_str = sql_ast_to_string(ctx, node->right);
                    return aml_pool_strdupf(ctx->pool, "(%s %s %s)", left_str, node->value, right_str);
                } else {
                    return aml_pool_strdupf(ctx->pool, "(%s %s)", left_str, node->value);
                }
            }
            if (strcasecmp(node->value, "IN") == 0 || strcasecmp(node->value, "NOT IN") == 0) {
                char *left = sql_ast_to_string(ctx, node->left);
                char *list = sql_ast_to_string(ctx, node->right);
                return aml_pool_strdupf(ctx->pool, "(%s %s %s)", left, node->value, list);
            }
            if (node->left && node->right) {
                char *left = sql_ast_to_string(ctx, node->left);
                char *right = sql_ast_to_string(ctx, node->right);
                return aml_pool_strdupf(ctx->pool, "(%s %s %s)", left, node->value, right);
            }
            if (node->left) {
                return aml_pool_strdupf(ctx->pool, "%s%s", node->value, sql_ast_to_string(ctx, node->left));
            }
            return aml_pool_strdup(ctx->pool, node->value);
        }
        case SQL_FUNCTION: {
            if (strcasecmp(node->value, "CASE") == 0) {
                char *res = aml_pool_strdup(ctx->pool, "CASE");
                sql_ast_node_t *curr = node->left;
                while (curr) {
                    if (strcasecmp(curr->value, "WHEN") == 0) {
                        char *cond = sql_ast_to_string(ctx, curr->left);
                        char *val = sql_ast_to_string(ctx, curr->right);
                        res = aml_pool_strdupf(ctx->pool, "%s WHEN %s THEN %s", res, cond, val);
                    } else if (strcasecmp(curr->value, "ELSE") == 0) {
                        char *val = sql_ast_to_string(ctx, curr->left);
                        res = aml_pool_strdupf(ctx->pool, "%s ELSE %s", res, val);
                    }
                    curr = curr->next;
                }
                return aml_pool_strdupf(ctx->pool, "%s END", res);
            }
            if (node->left && node->left->type == SQL_KEYWORD && strcasecmp(node->left->value, "FROM") == 0) {
                char *field = node->left->left->value;
                char *source = sql_ast_to_string(ctx, node->left->right);
                return aml_pool_strdupf(ctx->pool, "EXTRACT(%s FROM %s)", field, source);
            }
            if (strcasecmp(node->value, "POSITION") == 0 && node->left && node->left->next) {
                char *substr = sql_ast_to_string(ctx, node->left);
                char *str = sql_ast_to_string(ctx, node->left->next);
                return aml_pool_strdupf(ctx->pool, "POSITION(%s IN %s)", substr, str);
            }
            if (strcasecmp(node->value, "CAST") == 0 && node->left && node->left->next && node->left->next->next) {
                char *expr = sql_ast_to_string(ctx, node->left);
                char *type_str = sql_ast_to_string(ctx, node->left->next->next);
                return aml_pool_strdupf(ctx->pool, "CAST(%s AS %s)", expr, type_str);
            }
            char *args = ast_list_to_string(ctx, node->left);
            char *res = aml_pool_strdupf(ctx->pool, "%s(%s)", node->value, args);

            if (node->window_clause) {
                res = aml_pool_strdupf(ctx->pool, "%s OVER (", res);
                bool has_part = false;
                if (node->window_clause->partition_by) {
                    char *part_str = ast_list_to_string(ctx, node->window_clause->partition_by);
                    res = aml_pool_strdupf(ctx->pool, "%sPARTITION BY %s", res, part_str);
                    has_part = true;
                }
                if (node->window_clause->order_by) {
                    if (has_part) res = aml_pool_strdupf(ctx->pool, "%s ", res);
                    res = aml_pool_strdupf(ctx->pool, "%sORDER BY ", res);
                    sql_order_by_t *ob = node->window_clause->order_by;
                    while (ob) {
                        char *expr_str = sql_ast_to_string(ctx, ob->expr);
                        res = aml_pool_strdupf(ctx->pool, "%s%s %s", res, expr_str, ob->is_desc ? "DESC" : "ASC");
                        ob = ob->next;
                        if (ob) res = aml_pool_strdupf(ctx->pool, "%s, ", res);
                    }
                }
                res = aml_pool_strdupf(ctx->pool, "%s)", res);
            }

            return res;
        }
        case SQL_LIST: {
            char *args = ast_list_to_string(ctx, node->left);
            return aml_pool_strdupf(ctx->pool, "(%s)", args);
        }
        case SQL_EXISTS: {
            char *sub_str = sql_query_to_string(ctx, node->subquery);
            return aml_pool_strdupf(ctx->pool, "EXISTS (%s)", sub_str);
        }
        // Convert Subquery AST to String
        case SQL_NODE_SUBQUERY: {
            char *sub_str = sql_query_to_string(ctx, node->subquery);
            return aml_pool_strdupf(ctx->pool, "(%s)", sub_str);
        }
        default: break;
    }
    return aml_pool_strdup(ctx->pool, "");
}

char *sql_query_to_string(sql_ctx_t *ctx, sql_select_t *query) {
    if (!query) return NULL;

    char *sql = aml_pool_strdup(ctx->pool, "");

    if (query->is_explain) {
        sql = aml_pool_strdupf(ctx->pool, "EXPLAIN ");
    }

    if (query->ctes) {
        sql = aml_pool_strdupf(ctx->pool, "%sWITH ", sql);
        sql_cte_t *cte = query->ctes;
        while (cte) {
            char *cte_sql = sql_query_to_string(ctx, cte->query);
            sql = aml_pool_strdupf(ctx->pool, "%s%s AS (%s)%s", sql, cte->alias, cte_sql, cte->next ? ", " : " ");
            cte = cte->next;
        }
    }

    sql = aml_pool_strdupf(ctx->pool, "%sSELECT ", sql);

    if (query->is_star) {
        sql = aml_pool_strdupf(ctx->pool, "%s*", sql);
    } else {
        sql_ast_node_t *col = query->columns;
        while (col) {
            char *expr_str = sql_ast_to_string(ctx, col);
            if (col->alias) {
                sql = aml_pool_strdupf(ctx->pool, "%s%s AS %s", sql, expr_str, col->alias);
            } else {
                sql = aml_pool_strdupf(ctx->pool, "%s%s", sql, expr_str);
            }

            col = col->next;
            if (col) sql = aml_pool_strdupf(ctx->pool, "%s, ", sql);
        }
    }

    if (query->subquery) {
        char *sub_str = sql_query_to_string(ctx, query->subquery);
        sql = aml_pool_strdupf(ctx->pool, "%s FROM (%s)", sql, sub_str);
        if (query->table_alias) {
            sql = aml_pool_strdupf(ctx->pool, "%s AS %s", sql, query->table_alias);
        }
    } else if (query->table) {
        sql = aml_pool_strdupf(ctx->pool, "%s FROM %s", sql, query->table);
        if (query->table_alias) {
            sql = aml_pool_strdupf(ctx->pool, "%s AS %s", sql, query->table_alias);
        }
    }

    if (query->joins) {
        sql_join_t *j = query->joins;
        while (j) {
            const char *type_str = j->type == JOIN_LEFT ? "LEFT" :
                                   j->type == JOIN_RIGHT ? "RIGHT" :
                                   j->type == JOIN_FULL ? "FULL OUTER" : "INNER";

            if (j->subquery) {
                char *sub_str = sql_query_to_string(ctx, j->subquery);
                sql = aml_pool_strdupf(ctx->pool, "%s %s JOIN (%s)", sql, type_str, sub_str);
            } else {
                sql = aml_pool_strdupf(ctx->pool, "%s %s JOIN %s", sql, type_str, j->table);
            }

            if (j->alias) {
                sql = aml_pool_strdupf(ctx->pool, "%s AS %s", sql, j->alias);
            }

            char *on_str = sql_ast_to_string(ctx, j->on_condition);
            sql = aml_pool_strdupf(ctx->pool, "%s ON %s", sql, on_str);

            j = j->next;
        }
    }

    if (query->where_clause) {
        char *where_str = sql_ast_to_string(ctx, query->where_clause);
        sql = aml_pool_strdupf(ctx->pool, "%s WHERE %s", sql, where_str);
    }

    if (query->group_by) {
        char *gb_str = ast_list_to_string(ctx, query->group_by);
        sql = aml_pool_strdupf(ctx->pool, "%s GROUP BY %s", sql, gb_str);
    }

    if (query->having_clause) {
        char *having_str = sql_ast_to_string(ctx, query->having_clause);
        sql = aml_pool_strdupf(ctx->pool, "%s HAVING %s", sql, having_str);
    }

    if (query->order_by) {
        sql = aml_pool_strdupf(ctx->pool, "%s ORDER BY ", sql);
        sql_order_by_t *ob = query->order_by;
        while (ob) {
            char *expr_str = sql_ast_to_string(ctx, ob->expr);
            sql = aml_pool_strdupf(ctx->pool, "%s%s %s", sql, expr_str, ob->is_desc ? "DESC" : "ASC");

            ob = ob->next;
            if (ob) sql = aml_pool_strdupf(ctx->pool, "%s, ", sql);
        }
    }

    if (query->limit) {
        char *limit_str = sql_ast_to_string(ctx, query->limit);
        sql = aml_pool_strdupf(ctx->pool, "%s LIMIT %s", sql, limit_str);
    }

    if (query->offset) {
        char *offset_str = sql_ast_to_string(ctx, query->offset);
        sql = aml_pool_strdupf(ctx->pool, "%s OFFSET %s", sql, offset_str);
    }

    return sql;
}
