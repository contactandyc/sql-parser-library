// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0

#ifndef _sql_vm_H
#define _sql_vm_H

#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_query.h"
#include "sql-parser-library/sql_result_set.h"

typedef struct sql_vm_s sql_vm_t;
struct sql_dataset_s;
struct sql_node_s;

typedef enum {
    DS_MODE_MATERIALIZED,
    DS_MODE_STREAMING
} sql_dataset_mode_t;

typedef enum {
    INDEX_TYPE_HASH,
    INDEX_TYPE_BTREE
} sql_index_type_t;

typedef struct sql_index_s {
    const char **column_names;
    size_t num_columns;
    sql_index_type_t type;
    void *index_state;

    void **(*lookup_exact)(void *index_state, struct sql_node_s **vals, size_t num_vals, size_t *out_count);
    void **(*lookup_range)(void *index_state, struct sql_node_s **min_vals, struct sql_node_s **max_vals, size_t num_vals, size_t *out_count);
} sql_index_t;

typedef struct sql_dataset_s {
    const char *table_name;
    const char *alias;
    bool is_virtual;
    sql_dataset_mode_t mode;

    size_t count;
    void **rows;
    sql_result_set_t *rs;

    void *stream_state;
    bool (*next)(struct sql_dataset_s *ds, void **out_row);
    void (*rewind)(struct sql_dataset_s *ds);
    void (*close)(struct sql_dataset_s *ds);

    size_t num_columns;
    const char **column_names;
    sql_data_type_t *column_types;

    size_t num_indexes;
    sql_index_t **indexes;

    // --- NEW: Cloning Callback for O(1) Streaming Preservation ---
    void *(*clone_row)(struct sql_dataset_s *ds, void *row, aml_pool_t *persistent_pool);
} sql_dataset_t;

typedef sql_dataset_t *(*sql_vm_fetch_table_cb)(sql_vm_t *vm, const char *table_name);
typedef sql_ctx_column_t *(*sql_vm_resolve_column_cb)(sql_vm_t *vm, const char *table_name, const char *column_name);

struct sql_vm_s {
    sql_ctx_t *ctx;
    aml_pool_t *pool;
    sql_vm_fetch_table_cb fetch_table;
    sql_vm_resolve_column_cb resolve_column;
    void *user_data;
};

sql_vm_t *sql_vm_init(sql_ctx_t *ctx, sql_vm_fetch_table_cb fetch_cb, sql_vm_resolve_column_cb resolve_cb, void *user_data);
sql_dataset_t *sql_vm_create_materialized_dataset(sql_vm_t *vm, size_t count, void **rows);
sql_dataset_t *sql_vm_create_streaming_dataset(sql_vm_t *vm, void *stream_state,
                                               bool (*next_cb)(sql_dataset_t*, void**),
                                               void (*rewind_cb)(sql_dataset_t*),
                                               void (*close_cb)(sql_dataset_t*));
sql_result_set_t *sql_vm_execute(sql_vm_t *vm, sql_select_t *ast);

#endif /* _sql_vm_H */
