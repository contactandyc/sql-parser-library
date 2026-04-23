// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "a-json-library/ajson.h"
#include "a-memory-library/aml_pool.h"

#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_tokenizer.h"
#include "sql-parser-library/sql_query.h"
#include "sql-parser-library/sql_vm.h"

// --- 1. MOCK DATA ---
typedef struct { int id; const char *name; int age; } user_row_t;
typedef struct { int id; int user_id; double total; const char *status; } order_row_t;

user_row_t users[] = {
    {1, "Alice", 25}, {2, "Bob", 30}, {3, "Charlie", 35}, {4, "David", 40}
};
order_row_t orders[] = {
    {101, 1, 250.00, "shipped"}, {102, 1, 45.50, "pending"}, {103, 3, 899.99, "shipped"},
    {104, 4, 12.00, "shipped"}, {105, 1, 950.00, "shipped"}
};

const size_t num_users = sizeof(users) / sizeof(users[0]);
const size_t num_orders = sizeof(orders) / sizeof(orders[0]);

typedef enum { DS_USERS, DS_ORDERS } db_table_t;

// --- 2. HOST IMPLEMENTATION CALLBACKS ---

sql_dataset_t *my_fetch_table(sql_vm_t *vm, const char *table_name) {
    if (strcasecmp(table_name, "users") == 0) {
        void **rows = aml_pool_alloc(vm->pool, num_users * sizeof(void *));
        for(size_t i = 0; i < num_users; i++) rows[i] = &users[i];
        return sql_vm_create_materialized_dataset(vm, num_users, rows);
    }
    if (strcasecmp(table_name, "orders") == 0) {
        void **rows = aml_pool_alloc(vm->pool, num_orders * sizeof(void *));
        for(size_t i = 0; i < num_orders; i++) rows[i] = &orders[i];
        return sql_vm_create_materialized_dataset(vm, num_orders, rows);
    }
    return NULL;
}

sql_node_t *my_read_data(sql_ctx_t *ctx, sql_node_t *f) {
    void **row_set = (void **)ctx->row;
    if (!row_set || !f->column) return sql_bool_init(ctx, false, true);

    int t_idx = f->column->table_index;
    void *raw_row = row_set[t_idx];
    if (!raw_row) return sql_bool_init(ctx, false, true);

    const char *col = f->column->name;
    db_table_t type = (db_table_t)(uintptr_t)f->column->custom_data; // Fetch our injected tag

    if (type == DS_USERS) {
        user_row_t *u = (user_row_t *)raw_row;
        if (strcasecmp(col, "id") == 0) return sql_int_init(ctx, u->id, false);
        if (strcasecmp(col, "name") == 0) return sql_string_init(ctx, u->name, false);
        if (strcasecmp(col, "age") == 0) return sql_int_init(ctx, u->age, false);
    } else if (type == DS_ORDERS) {
        order_row_t *o = (order_row_t *)raw_row;
        if (strcasecmp(col, "id") == 0) return sql_int_init(ctx, o->id, false);
        if (strcasecmp(col, "user_id") == 0) return sql_int_init(ctx, o->user_id, false);
        if (strcasecmp(col, "total") == 0) return sql_double_init(ctx, o->total, false);
        if (strcasecmp(col, "status") == 0) return sql_string_init(ctx, o->status, false);
    }
    return sql_bool_init(ctx, false, true);
}

sql_ctx_column_t *my_resolve_col(sql_vm_t *vm, const char *table_name, const char *column_name) {
    sql_ctx_column_t *col = aml_pool_zalloc(vm->pool, sizeof(sql_ctx_column_t));
    col->name = aml_pool_strdup(vm->pool, column_name);
    col->func = my_read_data;

    if (strcasecmp(table_name, "users") == 0) {
        col->custom_data = (void *)(uintptr_t)DS_USERS; // Tag it so `my_read_data` knows what to cast
        if (strcasecmp(column_name, "id") == 0) col->type = SQL_TYPE_INT;
        else if (strcasecmp(column_name, "name") == 0) col->type = SQL_TYPE_STRING;
        else if (strcasecmp(column_name, "age") == 0) col->type = SQL_TYPE_INT;
        else return NULL; return col;
    } else if (strcasecmp(table_name, "orders") == 0) {
        col->custom_data = (void *)(uintptr_t)DS_ORDERS; // Tag it!
        if (strcasecmp(column_name, "id") == 0) col->type = SQL_TYPE_INT;
        else if (strcasecmp(column_name, "user_id") == 0) col->type = SQL_TYPE_INT;
        else if (strcasecmp(column_name, "total") == 0) col->type = SQL_TYPE_DOUBLE;
        else if (strcasecmp(column_name, "status") == 0) col->type = SQL_TYPE_STRING;
        else return NULL; return col;
    }
    return NULL;
}


// --- 3. EXECUTE! ---
int main(int argc, char **argv) {
    const char *query_str = (argc >= 2) ? argv[1] :
        "SELECT u.name AS customer, o.total "
        "FROM (SELECT id, name FROM users) u "
        "JOIN (SELECT user_id, SUM(total) AS total FROM orders GROUP BY user_id) o ON u.id = o.user_id "
        "ORDER BY o.total DESC";

    printf("Executing Query:\n%s\n\n", query_str);

    aml_pool_t *pool = aml_pool_init(1024 * 1024 * 10);
    sql_ctx_t context = {0};
    context.pool = pool;
    register_ctx(&context);

    // Initialize the Virtual Machine
    sql_vm_t *vm = sql_vm_init(&context, my_fetch_table, my_resolve_col, NULL);

    size_t token_count;
    sql_token_t **tokens = sql_tokenize(&context, query_str, &token_count);
    sql_select_t *query_ast = sql_parse_query(&context, tokens, token_count);

    sql_result_set_t *rs = sql_vm_execute(vm, query_ast);

    if (rs) {
        printf(">> Final Result Set (%zu rows):\n\n", rs->count);
        for (size_t r = 0; r < rs->count; r++) {
            printf("    ");
            for (size_t p = 0; p < rs->num_columns; p++) {
                printf("%s: ", rs->column_names[p]);
                sql_node_t *val = rs->rows[r].columns[p];

                if (val->is_null) printf("NULL");
                else switch (val->data_type) {
                    case SQL_TYPE_INT:      printf("%d", val->value.int_value); break;
                    case SQL_TYPE_DOUBLE:   printf("%.2f", val->value.double_value); break;
                    case SQL_TYPE_STRING:   printf("'%s'", val->value.string_value); break;
                    default: printf("???");
                }
                if (p < rs->num_columns - 1) printf(" | ");
            }
            printf("\n");
        }
    }

    printf("\n");
    aml_pool_destroy(pool);
    return 0;
}
