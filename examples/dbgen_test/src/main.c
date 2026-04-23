// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "a-memory-library/aml_pool.h"
#include "the-io-library/io.h"
#include "the-io-library/io_in.h"

#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_tokenizer.h"
#include "sql-parser-library/sql_query.h"
#include "sql-parser-library/sql_vm.h"

// ---> CHANGE THIS TO YOUR DBGEN FOLDER <---
const char *DATA_DIR = "/Users/ac/data/";

// --- 1. TPC-H SCHEMAS ---
typedef struct { int regionkey; const char *name; const char *comment; } region_t;
typedef struct { int nationkey; const char *name; int regionkey; const char *comment; } nation_t;
typedef struct { int suppkey; const char *name; const char *address; int nationkey; const char *phone; double acctbal; const char *comment; } supplier_t;
typedef struct { int partkey; const char *name; const char *mfgr; const char *brand; const char *type; int size; const char *container; double retailprice; const char *comment; } part_t;
typedef struct { int partkey; int suppkey; int availqty; double supplycost; const char *comment; } partsupp_t;
typedef struct { int custkey; const char *name; const char *address; int nationkey; const char *phone; double acctbal; const char *mktsegment; const char *comment; } customer_t;
typedef struct { int orderkey; int custkey; const char *orderstatus; double totalprice; const char *orderdate; const char *orderpriority; const char *clerk; int shippriority; const char *comment; } orders_t;
typedef struct { int orderkey; int partkey; int suppkey; int linenumber; double quantity; double extendedprice; double discount; double tax; const char *returnflag; const char *linestatus; const char *shipdate; const char *commitdate; const char *receiptdate; const char *shipinstruct; const char *shipmode; const char *comment; } lineitem_t;

size_t struct_sizes[] = {
    sizeof(region_t), sizeof(nation_t), sizeof(supplier_t), sizeof(part_t),
    sizeof(partsupp_t), sizeof(customer_t), sizeof(orders_t), sizeof(lineitem_t)
};

// --- 2. FAST COLUMN ACCESSOR MACROS ---
#define COL_INT(T_NAME, C_NAME) \
    static sql_node_t *col_##T_NAME##_##C_NAME(sql_ctx_t *ctx, sql_node_t *f) { \
        T_NAME##_t *r = (T_NAME##_t *)((void **)ctx->row)[f->column->table_index]; \
        if (!r) return sql_bool_init(ctx, false, true); \
        return sql_int_init(ctx, r->C_NAME, false); \
    }

#define COL_DBL(T_NAME, C_NAME) \
    static sql_node_t *col_##T_NAME##_##C_NAME(sql_ctx_t *ctx, sql_node_t *f) { \
        T_NAME##_t *r = (T_NAME##_t *)((void **)ctx->row)[f->column->table_index]; \
        if (!r) return sql_bool_init(ctx, false, true); \
        return sql_double_init(ctx, r->C_NAME, false); \
    }

#define COL_STR(T_NAME, C_NAME) \
    static sql_node_t *col_##T_NAME##_##C_NAME(sql_ctx_t *ctx, sql_node_t *f) { \
        T_NAME##_t *r = (T_NAME##_t *)((void **)ctx->row)[f->column->table_index]; \
        if (!r) return sql_bool_init(ctx, false, true); \
        return sql_string_init(ctx, r->C_NAME, false); \
    }

COL_INT(region, regionkey) COL_STR(region, name) COL_STR(region, comment)
COL_INT(nation, nationkey) COL_STR(nation, name) COL_INT(nation, regionkey) COL_STR(nation, comment)
COL_INT(supplier, suppkey) COL_STR(supplier, name) COL_STR(supplier, address) COL_INT(supplier, nationkey) COL_STR(supplier, phone) COL_DBL(supplier, acctbal) COL_STR(supplier, comment)
COL_INT(part, partkey) COL_STR(part, name) COL_STR(part, mfgr) COL_STR(part, brand) COL_STR(part, type) COL_INT(part, size) COL_STR(part, container) COL_DBL(part, retailprice) COL_STR(part, comment)
COL_INT(partsupp, partkey) COL_INT(partsupp, suppkey) COL_INT(partsupp, availqty) COL_DBL(partsupp, supplycost) COL_STR(partsupp, comment)
COL_INT(customer, custkey) COL_STR(customer, name) COL_STR(customer, address) COL_INT(customer, nationkey) COL_STR(customer, phone) COL_DBL(customer, acctbal) COL_STR(customer, mktsegment) COL_STR(customer, comment)
COL_INT(orders, orderkey) COL_INT(orders, custkey) COL_STR(orders, orderstatus) COL_DBL(orders, totalprice) COL_STR(orders, orderdate) COL_STR(orders, orderpriority) COL_STR(orders, clerk) COL_INT(orders, shippriority) COL_STR(orders, comment)
COL_INT(lineitem, orderkey) COL_INT(lineitem, partkey) COL_INT(lineitem, suppkey) COL_INT(lineitem, linenumber) COL_DBL(lineitem, quantity) COL_DBL(lineitem, extendedprice) COL_DBL(lineitem, discount) COL_DBL(lineitem, tax) COL_STR(lineitem, returnflag) COL_STR(lineitem, linestatus) COL_STR(lineitem, shipdate) COL_STR(lineitem, commitdate) COL_STR(lineitem, receiptdate) COL_STR(lineitem, shipinstruct) COL_STR(lineitem, shipmode) COL_STR(lineitem, comment)

// --- 3. DYNAMIC COLUMN ROUTER ---
#define BIND_COL(COL_STR, TYPE, FUNC) \
    if (strcasecmp(col, COL_STR) == 0) { c->type = TYPE; c->func = FUNC; return c; }

sql_ctx_column_t *my_resolve_col(sql_vm_t *vm, const char *table, const char *col) {
    sql_ctx_column_t *c = aml_pool_zalloc(vm->pool, sizeof(sql_ctx_column_t));
    c->name = aml_pool_strdup(vm->pool, col);

    if (strcasecmp(table, "region") == 0) { BIND_COL("regionkey", SQL_TYPE_INT, col_region_regionkey) BIND_COL("name", SQL_TYPE_STRING, col_region_name) BIND_COL("comment", SQL_TYPE_STRING, col_region_comment) }
    else if (strcasecmp(table, "nation") == 0) { BIND_COL("nationkey", SQL_TYPE_INT, col_nation_nationkey) BIND_COL("name", SQL_TYPE_STRING, col_nation_name) BIND_COL("regionkey", SQL_TYPE_INT, col_nation_regionkey) BIND_COL("comment", SQL_TYPE_STRING, col_nation_comment) }
    else if (strcasecmp(table, "supplier") == 0) { BIND_COL("suppkey", SQL_TYPE_INT, col_supplier_suppkey) BIND_COL("name", SQL_TYPE_STRING, col_supplier_name) BIND_COL("address", SQL_TYPE_STRING, col_supplier_address) BIND_COL("nationkey", SQL_TYPE_INT, col_supplier_nationkey) BIND_COL("phone", SQL_TYPE_STRING, col_supplier_phone) BIND_COL("acctbal", SQL_TYPE_DOUBLE, col_supplier_acctbal) BIND_COL("comment", SQL_TYPE_STRING, col_supplier_comment) }
    else if (strcasecmp(table, "part") == 0) { BIND_COL("partkey", SQL_TYPE_INT, col_part_partkey) BIND_COL("name", SQL_TYPE_STRING, col_part_name) BIND_COL("mfgr", SQL_TYPE_STRING, col_part_mfgr) BIND_COL("brand", SQL_TYPE_STRING, col_part_brand) BIND_COL("type", SQL_TYPE_STRING, col_part_type) BIND_COL("size", SQL_TYPE_INT, col_part_size) BIND_COL("container", SQL_TYPE_STRING, col_part_container) BIND_COL("retailprice", SQL_TYPE_DOUBLE, col_part_retailprice) BIND_COL("comment", SQL_TYPE_STRING, col_part_comment) }
    else if (strcasecmp(table, "partsupp") == 0) { BIND_COL("partkey", SQL_TYPE_INT, col_partsupp_partkey) BIND_COL("suppkey", SQL_TYPE_INT, col_partsupp_suppkey) BIND_COL("availqty", SQL_TYPE_INT, col_partsupp_availqty) BIND_COL("supplycost", SQL_TYPE_DOUBLE, col_partsupp_supplycost) BIND_COL("comment", SQL_TYPE_STRING, col_partsupp_comment) }
    else if (strcasecmp(table, "customer") == 0) { BIND_COL("custkey", SQL_TYPE_INT, col_customer_custkey) BIND_COL("name", SQL_TYPE_STRING, col_customer_name) BIND_COL("address", SQL_TYPE_STRING, col_customer_address) BIND_COL("nationkey", SQL_TYPE_INT, col_customer_nationkey) BIND_COL("phone", SQL_TYPE_STRING, col_customer_phone) BIND_COL("acctbal", SQL_TYPE_DOUBLE, col_customer_acctbal) BIND_COL("mktsegment", SQL_TYPE_STRING, col_customer_mktsegment) BIND_COL("comment", SQL_TYPE_STRING, col_customer_comment) }
    else if (strcasecmp(table, "orders") == 0) { BIND_COL("orderkey", SQL_TYPE_INT, col_orders_orderkey) BIND_COL("custkey", SQL_TYPE_INT, col_orders_custkey) BIND_COL("orderstatus", SQL_TYPE_STRING, col_orders_orderstatus) BIND_COL("totalprice", SQL_TYPE_DOUBLE, col_orders_totalprice) BIND_COL("orderdate", SQL_TYPE_STRING, col_orders_orderdate) BIND_COL("orderpriority", SQL_TYPE_STRING, col_orders_orderpriority) BIND_COL("clerk", SQL_TYPE_STRING, col_orders_clerk) BIND_COL("shippriority", SQL_TYPE_INT, col_orders_shippriority) BIND_COL("comment", SQL_TYPE_STRING, col_orders_comment) }
    else if (strcasecmp(table, "lineitem") == 0) { BIND_COL("orderkey", SQL_TYPE_INT, col_lineitem_orderkey) BIND_COL("partkey", SQL_TYPE_INT, col_lineitem_partkey) BIND_COL("suppkey", SQL_TYPE_INT, col_lineitem_suppkey) BIND_COL("linenumber", SQL_TYPE_INT, col_lineitem_linenumber) BIND_COL("quantity", SQL_TYPE_DOUBLE, col_lineitem_quantity) BIND_COL("extendedprice", SQL_TYPE_DOUBLE, col_lineitem_extendedprice) BIND_COL("discount", SQL_TYPE_DOUBLE, col_lineitem_discount) BIND_COL("tax", SQL_TYPE_DOUBLE, col_lineitem_tax) BIND_COL("returnflag", SQL_TYPE_STRING, col_lineitem_returnflag) BIND_COL("linestatus", SQL_TYPE_STRING, col_lineitem_linestatus) BIND_COL("shipdate", SQL_TYPE_STRING, col_lineitem_shipdate) BIND_COL("commitdate", SQL_TYPE_STRING, col_lineitem_commitdate) BIND_COL("receiptdate", SQL_TYPE_STRING, col_lineitem_receiptdate) BIND_COL("shipinstruct", SQL_TYPE_STRING, col_lineitem_shipinstruct) BIND_COL("shipmode", SQL_TYPE_STRING, col_lineitem_shipmode) BIND_COL("comment", SQL_TYPE_STRING, col_lineitem_comment) }
    return NULL;
}

// --- 4. UNIFIED PARSER ---
static inline char *nxt(char **ptr) { return strsep(ptr, "|\n\r"); }

void parse_tpch_row(int type, char *line, void *row) {
    char *ptr = line;
    if (type == 0) { region_t *r = (region_t *)row; r->regionkey = atoi(nxt(&ptr)); r->name = nxt(&ptr); r->comment = nxt(&ptr); }
    else if (type == 1) { nation_t *r = (nation_t *)row; r->nationkey = atoi(nxt(&ptr)); r->name = nxt(&ptr); r->regionkey = atoi(nxt(&ptr)); r->comment = nxt(&ptr); }
    else if (type == 2) { supplier_t *r = (supplier_t *)row; r->suppkey = atoi(nxt(&ptr)); r->name = nxt(&ptr); r->address = nxt(&ptr); r->nationkey = atoi(nxt(&ptr)); r->phone = nxt(&ptr); r->acctbal = atof(nxt(&ptr)); r->comment = nxt(&ptr); }
    else if (type == 3) { part_t *r = (part_t *)row; r->partkey = atoi(nxt(&ptr)); r->name = nxt(&ptr); r->mfgr = nxt(&ptr); r->brand = nxt(&ptr); r->type = nxt(&ptr); r->size = atoi(nxt(&ptr)); r->container = nxt(&ptr); r->retailprice = atof(nxt(&ptr)); r->comment = nxt(&ptr); }
    else if (type == 4) { partsupp_t *r = (partsupp_t *)row; r->partkey = atoi(nxt(&ptr)); r->suppkey = atoi(nxt(&ptr)); r->availqty = atoi(nxt(&ptr)); r->supplycost = atof(nxt(&ptr)); r->comment = nxt(&ptr); }
    else if (type == 5) { customer_t *r = (customer_t *)row; r->custkey = atoi(nxt(&ptr)); r->name = nxt(&ptr); r->address = nxt(&ptr); r->nationkey = atoi(nxt(&ptr)); r->phone = nxt(&ptr); r->acctbal = atof(nxt(&ptr)); r->mktsegment = nxt(&ptr); r->comment = nxt(&ptr); }
    else if (type == 6) { orders_t *r = (orders_t *)row; r->orderkey = atoi(nxt(&ptr)); r->custkey = atoi(nxt(&ptr)); r->orderstatus = nxt(&ptr); r->totalprice = atof(nxt(&ptr)); r->orderdate = nxt(&ptr); r->orderpriority = nxt(&ptr); r->clerk = nxt(&ptr); r->shippriority = atoi(nxt(&ptr)); r->comment = nxt(&ptr); }
    else if (type == 7) { lineitem_t *r = (lineitem_t *)row; r->orderkey = atoi(nxt(&ptr)); r->partkey = atoi(nxt(&ptr)); r->suppkey = atoi(nxt(&ptr)); r->linenumber = atoi(nxt(&ptr)); r->quantity = atof(nxt(&ptr)); r->extendedprice = atof(nxt(&ptr)); r->discount = atof(nxt(&ptr)); r->tax = atof(nxt(&ptr)); r->returnflag = nxt(&ptr); r->linestatus = nxt(&ptr); r->shipdate = nxt(&ptr); r->commitdate = nxt(&ptr); r->receiptdate = nxt(&ptr); r->shipinstruct = nxt(&ptr); r->shipmode = nxt(&ptr); r->comment = nxt(&ptr); }
}

// --- 5. IO-LIBRARY STREAM BINDINGS ---
typedef struct {
    io_in_t *in;
    char filename[256];
    void *current_row;
    int table_type;
} tpch_stream_t;

bool tpch_stream_next(sql_dataset_t *ds, void **out_row) {
    tpch_stream_t *s = (tpch_stream_t *)ds->stream_state;
    if (!s->in) return false;

    io_record_t *rec;
    while ((rec = io_in_advance(s->in)) != NULL) {
        if (rec->length < 5) continue;

        rec->record[rec->length] = '\0';
        parse_tpch_row(s->table_type, rec->record, s->current_row);

        *out_row = s->current_row;
        return true;
    }
    return false;
}

void tpch_stream_rewind(sql_dataset_t *ds) {
    tpch_stream_t *s = (tpch_stream_t *)ds->stream_state;
    if (s->in) io_in_destroy(s->in);
    s->in = io_in_quick_init(s->filename, io_delimiter('\n'), 1024 * 1024 * 4);
}

void tpch_stream_close(sql_dataset_t *ds) {
    tpch_stream_t *s = (tpch_stream_t *)ds->stream_state;
    if (s->in) io_in_destroy(s->in);
}

// --- 6. DUAL-MODE DYNAMIC TABLE LOADER ---
sql_dataset_t *my_fetch_table(sql_vm_t *vm, const char *table) {
    int t_idx = -1;
    if (!strcasecmp(table, "region")) t_idx = 0; else if (!strcasecmp(table, "nation")) t_idx = 1;
    else if (!strcasecmp(table, "supplier")) t_idx = 2; else if (!strcasecmp(table, "part")) t_idx = 3;
    else if (!strcasecmp(table, "partsupp")) t_idx = 4; else if (!strcasecmp(table, "customer")) t_idx = 5;
    else if (!strcasecmp(table, "orders")) t_idx = 6; else if (!strcasecmp(table, "lineitem")) t_idx = 7;
    if (t_idx == -1) return NULL;

    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s%s.tbl", DATA_DIR, table);

    size_t file_size = io_file_size(filepath);
    if (file_size == 0 && !io_file_exists(filepath)) return NULL;

    // The Magic Threshold: 25 MB!
    if (file_size < 25 * 1024 * 1024) {
        printf("[SCHEMA] Loading '%s' into Memory (Size: %zu bytes)\n", table, file_size);
        size_t len;
        char *data = io_pool_read_file(vm->pool, &len, filepath);

        size_t lines = 0;
        for (size_t i = 0; i < len; i++) if (data[i] == '\n') lines++;

        void **row_ptrs = aml_pool_alloc(vm->pool, lines * sizeof(void *));
        char *structs = aml_pool_alloc(vm->pool, lines * struct_sizes[t_idx]);

        char *ptr = data;
        for (size_t i = 0; i < lines; i++) {
            char *line = strsep(&ptr, "\n");
            if (!line || *line == '\0') break;
            void *row = structs + (i * struct_sizes[t_idx]);
            parse_tpch_row(t_idx, line, row);
            row_ptrs[i] = row;
        }
        return sql_vm_create_materialized_dataset(vm, lines, row_ptrs);
    }
    else {
        printf("[SCHEMA] Streaming '%s' from Disk (Size: %zu bytes)\n", table, file_size);
        tpch_stream_t *stream = aml_pool_zalloc(vm->pool, sizeof(tpch_stream_t));
        strcpy(stream->filename, filepath);
        stream->table_type = t_idx;
        stream->current_row = aml_pool_zalloc(vm->pool, struct_sizes[t_idx]);

        sql_dataset_t *ds = sql_vm_create_streaming_dataset(vm, stream, tpch_stream_next, tpch_stream_rewind, tpch_stream_close);

        ds->count = file_size;
        return ds;
    }
}

// --- 7. EXECUTION ---
int main(int argc, char **argv) {
    const char *queries[] = {
        // Test 1: Simple Scan & Filter
        "SELECT name, comment "
        "FROM region "
        "WHERE name = 'AMERICA'",

        // Test 2: Joins, Sorting, Limit, and Offset
        "SELECT n.name AS Nation, r.name AS Region "
        "FROM nation n "
        "JOIN region r ON n.regionkey = r.regionkey "
        "ORDER BY Nation ASC "
        "LIMIT 5 OFFSET 2",

        // Test 3: Aggregation & Grouping
        "SELECT r.name AS Region, SUM(n.nationkey) AS NationKeySum "
        "FROM nation n "
        "JOIN region r ON n.regionkey = r.regionkey "
        "GROUP BY r.name "
        "ORDER BY Region ASC",

        // Test 4: Aggregation with HAVING
        "SELECT r.name AS Region, SUM(n.nationkey) AS NationKeySum "
        "FROM nation n "
        "JOIN region r ON n.regionkey = r.regionkey "
        "GROUP BY r.name "
        "HAVING NationKeySum > 10 "
        "ORDER BY Region DESC",

        // Test 5: The 4-Table TPC-H Beast
        "SELECT n.name AS Nation, SUM(l.extendedprice * (1 - l.discount)) AS Revenue "
        "FROM part p "
        "JOIN lineitem l ON p.partkey = l.partkey "
        "JOIN supplier s ON l.suppkey = s.suppkey "
        "JOIN nation n ON s.nationkey = n.nationkey "
        "WHERE p.mfgr = 'Manufacturer#1' "
        "GROUP BY n.name "
        "ORDER BY Revenue DESC "
        "LIMIT 10",

        // Test 6: Common Table Expressions (WITH) Support!
        "WITH regional_nations AS ( "
        "    SELECT n.name AS Nation, r.name AS Region, n.nationkey "
        "    FROM nation n "
        "    JOIN region r ON n.regionkey = r.regionkey "
        "    WHERE r.name = 'ASIA' "
        ") "
        "SELECT Region, SUM(nationkey) AS SumKey "
        "FROM regional_nations "
        "GROUP BY Region"
    };

    size_t num_queries = sizeof(queries) / sizeof(queries[0]);

    for (size_t i = 0; i < num_queries; i++) {
        printf("\n======================================================\n");
        printf("Executing Query %zu:\n%s\n", i + 1, queries[i]);
        printf("======================================================\n");

        aml_pool_t *pool = aml_pool_init(1024 * 1024 * 50);
        sql_ctx_t context = {0};
        context.pool = pool;
        register_ctx(&context);

        sql_vm_t *vm = sql_vm_init(&context, my_fetch_table, my_resolve_col, NULL);

        size_t token_count;
        sql_token_t **tokens = sql_tokenize(&context, queries[i], &token_count);
        sql_select_t *query_ast = sql_parse_query(&context, tokens, token_count);

        sql_result_set_t *rs = sql_vm_execute(vm, query_ast);

        if (rs) {
            printf("\n>> Final Result Set (%zu rows):\n\n", rs->count);
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
        } else {
            printf("\nQuery failed or returned empty dataset.\n");
            sql_ctx_print_messages(&context);
        }

        printf("\n");
        aml_pool_destroy(pool);
    }

    return 0;
}