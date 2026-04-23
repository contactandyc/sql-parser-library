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

// --- NEW: USER PROVIDED INDEX INTERFACE ---
typedef enum {
    INDEX_TYPE_HASH,   // O(1) exact match only
    INDEX_TYPE_BTREE   // O(log n) exact match, range scans, and ordered traversal
} sql_index_type_t;

typedef struct sql_index_s {
    const char **column_names; // e.g., ["last_name", "first_name"]
    size_t num_columns;        // e.g., 2

    sql_index_type_t type;
    void *index_state;         // The user's internal map/tree pointer

    // lookup_exact receives an array of perfectly matched values
    void **(*lookup_exact)(void *index_state,
                           struct sql_node_s **vals,
                           size_t num_vals,
                           size_t *out_count);

    // lookup_range receives min/max boundaries (which can be NULL for open ranges)
    void **(*lookup_range)(void *index_state,
                           struct sql_node_s **min_vals,
                           struct sql_node_s **max_vals,
                           size_t num_vals,
                           size_t *out_count);
} sql_index_t;

// Represents a data source (Physical Table OR Virtual Subquery)
typedef struct sql_dataset_s {
    const char *table_name; // The physical schema name
    const char *alias;      // The query alias (e.g. 'p')
    bool is_virtual;
    sql_dataset_mode_t mode;

    // --- Mode 1: Materialized (In-Memory) ---
    size_t count;
    void **rows;
    sql_result_set_t *rs;

    // --- Mode 2: Streaming (Iterators) ---
    void *stream_state;
    bool (*next)(struct sql_dataset_s *ds, void **out_row);
    void (*rewind)(struct sql_dataset_s *ds);
    void (*close)(struct sql_dataset_s *ds);

    // Schema Metadata
    size_t num_columns;
    const char **column_names;
    sql_data_type_t *column_types;

    // --- NEW: Attached Indexes ---
    size_t num_indexes;
    sql_index_t **indexes;
} sql_dataset_t;

// Host application callbacks
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
