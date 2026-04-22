// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <string.h>
#include "sql-parser-library/sql_tokenizer.h"
#include "sql-parser-library/sql_ast.h"
#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_query.h"
#include "sql-parser-library/sql_binder.h"
#include "sql-parser-library/sql_planner.h"
#include "sql-parser-library/date_utils.h"
#include "a-memory-library/aml_pool.h"

// --- 1. MOCK DATA STRUCTURES ---

typedef struct {
    int id;
    const char *name;
    int age;
} user_row_t;

typedef struct {
    int id;
    int user_id;
    double total;
    const char *status;
} order_row_t;

user_row_t users[] = {
    {1, "Alice",   25},
    {2, "Bob",     30},
    {3, "Charlie", 35},
    {4, "David",   40}
};

order_row_t orders[] = {
    {101, 1, 250.00, "shipped"},
    {102, 1, 45.50,  "pending"},
    {103, 3, 899.99, "shipped"},
    {104, 4, 12.00,  "shipped"}
};

const size_t num_users = sizeof(users) / sizeof(users[0]);
const size_t num_orders = sizeof(orders) / sizeof(orders[0]);

// --- 2. DATA GETTER (The execution callback) ---

sql_node_t *sql_func_get_data(sql_ctx_t *ctx, sql_node_t *f) {
    void **row_set = (void **)ctx->row;

    if (!row_set || !f->column) return sql_bool_init(ctx, false, true);

    int t_idx = f->column->table_index;
    void *raw_row = row_set[t_idx];
    if (!raw_row) return sql_bool_init(ctx, false, true);

    const char *col_name = f->column->name;

    // Handle 'users' table
    if (t_idx == 0) {
        user_row_t *u = (user_row_t *)raw_row;
        if (strcasecmp(col_name, "id") == 0) return sql_int_init(ctx, u->id, false);
        if (strcasecmp(col_name, "name") == 0) return sql_string_init(ctx, u->name, false);
        if (strcasecmp(col_name, "age") == 0) return sql_int_init(ctx, u->age, false);
    }
    // Handle 'orders' table
    else if (t_idx == 1) {
        order_row_t *o = (order_row_t *)raw_row;
        if (strcasecmp(col_name, "id") == 0) return sql_int_init(ctx, o->id, false);
        if (strcasecmp(col_name, "user_id") == 0) return sql_int_init(ctx, o->user_id, false);
        if (strcasecmp(col_name, "total") == 0) return sql_double_init(ctx, o->total, false);
        if (strcasecmp(col_name, "status") == 0) return sql_string_init(ctx, o->status, false);
    }

    return sql_bool_init(ctx, false, true);
}


// --- 3. DYNAMIC SCHEMA CATALOG ---

// The Binder calls this function whenever it needs to verify a table/column exists
sql_ctx_column_t *my_dynamic_catalog(sql_ctx_t *ctx, const char *table_name, const char *column_name) {
    if (!table_name) table_name = "users"; // Fallback for raw standalone expressions

    sql_ctx_column_t *col = aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_column_t));
    col->name = aml_pool_strdup(ctx->pool, column_name);
    col->table_name = aml_pool_strdup(ctx->pool, table_name);

    // Assign the data retrieval function pointer dynamically!
    col->func = sql_func_get_data;

    if (strcasecmp(table_name, "users") == 0) {
        if (strcasecmp(column_name, "id") == 0) col->type = SQL_TYPE_INT;
        else if (strcasecmp(column_name, "name") == 0) col->type = SQL_TYPE_STRING;
        else if (strcasecmp(column_name, "age") == 0) col->type = SQL_TYPE_INT;
        else return NULL; // Column not found
        return col;
    }
    else if (strcasecmp(table_name, "orders") == 0) {
        if (strcasecmp(column_name, "id") == 0) col->type = SQL_TYPE_INT;
        else if (strcasecmp(column_name, "user_id") == 0) col->type = SQL_TYPE_INT;
        else if (strcasecmp(column_name, "total") == 0) col->type = SQL_TYPE_DOUBLE;
        else if (strcasecmp(column_name, "status") == 0) col->type = SQL_TYPE_STRING;
        else return NULL; // Column not found
        return col;
    }

    return NULL; // Table not found
}


// --- HELPER FUNCTIONS ---

sql_node_t *compile_expression(sql_ctx_t *ctx, sql_ast_node_t *ast) {
    if (!ast) return NULL;
    sql_node_t *node = convert_ast_to_node(ctx, ast);
    apply_type_conversions(ctx, node);
    simplify_func_tree(ctx, node);
    simplify_logical_expressions(node);
    return node;
}

sql_table_request_t *get_table_request(sql_execution_plan_t *plan, const char *name) {
    sql_table_request_t *req = plan->table_requests;
    while (req) {
        if (strcasecmp(req->table_name, name) == 0) return req;
        req = req->next;
    }
    return NULL;
}

typedef struct {
    sql_node_t *expr_node;
    const char *display_name;
} compiled_projection_t;


// --- 4. MAIN PIPELINE ---

int main(int argc, char **argv) {
    const char *query_str = (argc >= 2) ? argv[1] :
        "SELECT u.name AS customer, o.total, (u.age * 2) AS double_age "
        "FROM users u "
        "JOIN orders o ON u.id = o.user_id "
        "WHERE double_age >= 50 AND o.status = 'shipped'";

    printf("Executing Query:\n%s\n\n", query_str);

    // Context Setup
    aml_pool_t *pool = aml_pool_init(1024 * 1024);
    sql_ctx_t context = {0};
    context.pool = pool;

    // Wire up the Dynamic Catalog instead of a static array!
    context.schema_lookup = my_dynamic_catalog;

    register_ctx(&context);

    // 1. Parsing
    size_t token_count;
    sql_token_t **tokens = sql_tokenize(&context, query_str, &token_count);
    sql_select_t *query_ast = sql_parse_query(&context, tokens, token_count);

    if (!query_ast) {
        printf("Syntax Error: Failed to parse query.\n");
        sql_ctx_print_messages(&context);
        return 1;
    }

    // 2. Binding
    if (!sql_bind_query_extended(&context, query_ast)) {
        printf("Semantic Error: Failed to bind the query.\n");
        sql_ctx_print_messages(&context);
        return 1;
    }

    // 3. Planning & Pushdown Optimization
    sql_execution_plan_t *plan = sql_plan_query(&context, query_ast);
    sql_pushdown_filters(&context, plan);

    // 4. Compile Execution Nodes
    sql_table_request_t *user_req = get_table_request(plan, "users");
    sql_table_request_t *order_req = get_table_request(plan, "orders");

    sql_node_t *users_local_filter = user_req ? compile_expression(&context, user_req->table_filters) : NULL;
    sql_node_t *orders_local_filter = order_req ? compile_expression(&context, order_req->table_filters) : NULL;
    sql_node_t *join_on_condition = plan->joins ? compile_expression(&context, plan->joins->on_condition) : NULL;
    sql_node_t *global_filter = compile_expression(&context, plan->global_filters);

    // Compile Projections (SELECT List)
    size_t num_projections = 0;
    sql_ast_node_t *curr_col = query_ast->columns;
    while (curr_col) { num_projections++; curr_col = curr_col->next; }

    compiled_projection_t *projections = aml_pool_alloc(pool, num_projections * sizeof(compiled_projection_t));
    curr_col = query_ast->columns;
    for (size_t i = 0; i < num_projections; i++) {
        projections[i].expr_node = compile_expression(&context, curr_col);
        projections[i].display_name = curr_col->alias ? curr_col->alias : (curr_col->value ? curr_col->value : "expr");
        curr_col = curr_col->next;
    }

    // 5. Execution Engine Loop
    printf(">> Execution Trace:\n\n");

    void *current_row_set[2] = {0};
    context.row = current_row_set;

    for (size_t u = 0; u < num_users; u++) {
        current_row_set[0] = &users[u];

        if (users_local_filter) {
            sql_node_t *res = sql_eval(&context, users_local_filter);
            if (!res || res->is_null || !res->value.bool_value) continue;
        }

        for (size_t o = 0; o < num_orders; o++) {
            current_row_set[1] = &orders[o];

            if (orders_local_filter) {
                sql_node_t *res = sql_eval(&context, orders_local_filter);
                if (!res || res->is_null || !res->value.bool_value) continue;
            }

            if (join_on_condition) {
                sql_node_t *res = sql_eval(&context, join_on_condition);
                if (!res || res->is_null || !res->value.bool_value) continue;
            }

            if (global_filter) {
                sql_node_t *res = sql_eval(&context, global_filter);
                if (!res || res->is_null || !res->value.bool_value) continue;
            }

            // --- WE HAVE A MATCH! Evaluate Projections ---
            printf("    -> ROW MATCH | ");
            for (size_t p = 0; p < num_projections; p++) {
                sql_node_t *val = sql_eval(&context, projections[p].expr_node);

                printf("%s: ", projections[p].display_name);

                if (!val || val->is_null) {
                    printf("NULL");
                } else {
                    switch (val->data_type) {
                        case SQL_TYPE_INT:      printf("%d", val->value.int_value); break;
                        case SQL_TYPE_DOUBLE:   printf("%.2f", val->value.double_value); break;
                        case SQL_TYPE_STRING:   printf("'%s'", val->value.string_value); break;
                        case SQL_TYPE_BOOL:     printf("%s", val->value.bool_value ? "true" : "false"); break;
                        case SQL_TYPE_DATETIME: {
                            char *iso = convert_epoch_to_iso_utc(pool, val->value.epoch);
                            printf("'%s'", iso ? iso : "ERR");
                            break;
                        }
                        default: printf("UNKNOWN");
                    }
                }
                if (p < num_projections - 1) printf(" | ");
            }
            printf("\n");
        }
    }

    printf("\n");
    sql_ctx_print_messages(&context);
    aml_pool_destroy(pool);
    return 0;
}
