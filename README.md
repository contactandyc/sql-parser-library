# sql-parser-library

> A lightweight, embeddable C library for tokenizing, parsing, planning, compiling, and executing full SQL queries against your own custom data structures via a custom Virtual Machine.

**Version: 0.1.8**

---

## Table of Contents

1. [Overview](#overview)
2. [Key Features](#key-features)
3. [Integration & Quickstart](#integration--quickstart)
4. [Usage Example (Querying C Structs)](#usage-example-querying-c-structs)
5. [Documentation](#documentation)
6. [License](#license)
7. [Attribution](#attribution)

---

## Overview

`sql-parser-library` is a full, embeddable SQL execution engine. It allows you to expose your application's internal data—whether it resides in memory arrays, JSON documents, or streaming CSV files—and query it using standard SQL.

The library handles the complex pipeline of parsing, building execution plans, optimizing join orders, pushing down filters, and executing the query through its Virtual Machine (VM). You simply tell the VM how to read your data.

---

## Key Features

* **Advanced SQL Support:** Handles `JOIN`s, `GROUP BY`, `HAVING`, Common Table Expressions (`WITH`), Subqueries, and Window Functions (`OVER (PARTITION BY ...)`).
* **Zero-Overhead Memory:** Uses arena allocators (`aml_pool_t`) for instantaneous garbage collection. No memory leaks, no scattered `free()` calls.
* **Bring Your Own Data:** The engine has no concept of physical storage. You integrate it by providing callbacks that read from your structs, streams, or files.
* **Streaming vs. Materialized:** Load tiny tables entirely into memory for speed, or stream massive files row-by-row to keep your memory footprint strictly bounded.
* **Pluggable Standard Library:** Comes packed with dozens of standard SQL functions (String manipulation, Date/Time math, Math, JSON extraction) that can be easily extended.
* **Query Optimizer:** Automatically selects Hash Joins vs. Nested Loops, pushes `WHERE` filters down to the storage layer, and utilizes custom B-Tree/Hash indexes if you provide them.

---

## Integration & Quickstart

To use the library, link it against your project and include the primary headers. The engine relies heavily on the `a-memory-library` (for arena allocation) which must also be included in your build.

```c
#include "a-memory-library/aml_pool.h"
#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_tokenizer.h"
#include "sql-parser-library/sql_query.h"
#include "sql-parser-library/sql_vm.h"
```

---

## Usage Example (Querying C Structs)

Here is a complete "Hello World" example showing how to expose a standard C array of structs to the SQL Virtual Machine and query it.

### 1. Define your data
```c
typedef struct { 
    int id; 
    const char *name; 
    int age; 
} user_t;

user_t my_users[] = {
    {1, "Alice", 25}, 
    {2, "Bob", 30}, 
    {3, "Charlie", 35}
};
size_t num_users = 3;
```

### 2. Tell the VM how to fetch the table
```c
sql_dataset_t *my_fetch_table(sql_vm_t *vm, const char *table_name) {
    if (strcasecmp(table_name, "users") == 0) {
        // Create an array of pointers to our structs
        void **rows = aml_pool_alloc(vm->pool, num_users * sizeof(void *));
        for(size_t i = 0; i < num_users; i++) rows[i] = &my_users[i];
        
        // Hand it to the VM as a materialized (in-memory) dataset
        return sql_vm_create_materialized_dataset(vm, num_users, rows);
    }
    return NULL; // Table not found
}
```

### 3. Tell the VM how to read columns from your struct
To maximize performance, we bind a specific, fast accessor function to each column. This ensures the VM can retrieve data in $O(1)$ time without doing any string comparisons during query execution.

```c
// --- Fast, direct column accessors ---
static sql_node_t *read_user_id(sql_ctx_t *ctx, sql_node_t *f) {
    user_t *u = (user_t *)((void **)ctx->row)[f->column->table_index];
    return u ? sql_int_init(ctx, u->id, false) : sql_bool_init(ctx, false, true);
}

static sql_node_t *read_user_name(sql_ctx_t *ctx, sql_node_t *f) {
    user_t *u = (user_t *)((void **)ctx->row)[f->column->table_index];
    return u ? sql_string_init(ctx, u->name, false) : sql_bool_init(ctx, false, true);
}

static sql_node_t *read_user_age(sql_ctx_t *ctx, sql_node_t *f) {
    user_t *u = (user_t *)((void **)ctx->row)[f->column->table_index];
    return u ? sql_int_init(ctx, u->age, false) : sql_bool_init(ctx, false, true);
}

// --- The column schema resolver ---
sql_ctx_column_t *my_resolve_col(sql_vm_t *vm, const char *table, const char *col) {
    if (strcasecmp(table, "users") != 0) return NULL;

    sql_ctx_column_t *c = aml_pool_zalloc(vm->pool, sizeof(sql_ctx_column_t));
    c->name = aml_pool_strdup(vm->pool, col);

    // Bind the exact type and the specific reader function
    if (strcasecmp(col, "id") == 0) {
        c->type = SQL_TYPE_INT;
        c->func = read_user_id;
    } else if (strcasecmp(col, "name") == 0) {
        c->type = SQL_TYPE_STRING;
        c->func = read_user_name;
    } else if (strcasecmp(col, "age") == 0) {
        c->type = SQL_TYPE_INT;
        c->func = read_user_age;
    } else {
        return NULL; // Column not found
    }

    return c;
}
```

### 4. Execute a Query!
```c
int main() {
    // Initialize memory and context
    aml_pool_t *pool = aml_pool_init(1024 * 1024);
    sql_ctx_t ctx = { .pool = pool };
    sql_register_ctx(&ctx); // Load standard library functions (SUM, CONCAT, etc.)

    // Initialize the VM with our custom callbacks
    sql_vm_t *vm = sql_vm_init(&ctx, my_fetch_table, my_resolve_col, NULL);

    // The query
    const char *sql = "SELECT name, age FROM users WHERE age >= 30 ORDER BY name DESC;";

    // Tokenize and Parse
    size_t tokens_len = 0;
    sql_token_t **tokens = sql_tokenize(&ctx, sql, &tokens_len);
    sql_select_t *ast = sql_parse_query(&ctx, tokens, tokens_len);

    // Execute!
    sql_result_set_t *rs = sql_vm_execute(vm, ast);

    // Print Results
    if (rs) {
        for (size_t r = 0; r < rs->count; r++) {
            for (size_t p = 0; p < rs->num_columns; p++) {
                sql_node_t *val = rs->rows[r].columns[p];
                if (val->data_type == SQL_TYPE_STRING) printf("%s | ", val->value.string_value);
                if (val->data_type == SQL_TYPE_INT) printf("%d | ", val->value.int_value);
            }
            printf("\n");
        }
    } else {
        sql_ctx_print_messages(&ctx); // Print syntax or execution errors
    }

    aml_pool_destroy(pool); // Cleans up EVERYTHING (AST, VM, Results) instantly
    return 0;
}
```

---

## Documentation

For a deep dive into how the engine works under the hood—including the pipeline flow, memory management architecture, building custom Aggregate/Window function specifications, and writing streaming dataset implementations—please refer to the **[ARCHITECTURE.md](ARCHITECTURE.md)** file.

---

## License

Licensed under the Apache License, Version 2.0. See SPDX headers for details. Ensure any distribution retains existing SPDX notices.

---

## Attribution

Maintainer: **Andy Curtis [contactandyc@gmail.com](mailto:contactandyc@gmail.com)** Copyright © 2024-2026 Andy Curtis.  
*Portions Copyright © 2024-2025 Knode.ai.*
