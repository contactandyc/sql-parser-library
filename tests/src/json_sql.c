// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "a-json-library/ajson.h"
#include "a-memory-library/aml_pool.h"

// Include your existing SQL-related headers
#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_node.h"
#include "sql-parser-library/sql_ast.h"
#include "sql-parser-library/sql_tokenizer.h"

//----------------------------------------
// Minimal structure for storing row data
//----------------------------------------
typedef struct my_row_s {
    char **values;  // dynamically sized array of "column values" as strings
} my_row_t;

//----------------------------------------
// We'll store a "my_table_t" in memory
//----------------------------------------
typedef struct my_table_s {
    char *table_name;
    sql_ctx_column_t *columns;  // array of columns
    size_t num_columns;

    my_row_t *rows;      // array of row data
    size_t num_rows;
} my_table_t;

//----------------------------------------
// We need a memory pool to pass around
//----------------------------------------
static aml_pool_t *g_pool = NULL;

//----------------------------------------
// For each column, define a callback
// that reads from our `my_row_t` via context->row
//----------------------------------------
static sql_node_t *my_col_getter(sql_ctx_t *ctx, sql_node_t *f)
{
    my_row_t *row = (my_row_t *)ctx->row;

    // Safely look up the column index by matching f->token (the column name)
    // against the context's column definitions.
    int col_index = -1;
    for (size_t i = 0; i < ctx->column_count; i++) {
        if (strcasecmp(ctx->columns[i].name, f->token) == 0) {
            col_index = i;
            break;
        }
    }

    if (col_index < 0) {
        // Column not found, return NULL literal
        return sql_string_init(ctx, NULL, true);
    }

    const char *val = row->values[col_index];
    if (!val) val = "";

    switch (f->data_type) {
        case SQL_TYPE_INT: {
            int ival = atoi(val);
            return sql_int_init(ctx, ival, false);
        }
        case SQL_TYPE_DOUBLE: {
            double dval = atof(val);
            return sql_double_init(ctx, dval, false);
        }
        case SQL_TYPE_DATETIME: {
            time_t tval = (time_t)atol(val);
            return sql_datetime_init(ctx, tval, (tval == 0));
        }
        case SQL_TYPE_BOOL: {
            bool bval = (strcasecmp(val, "true") == 0 || strcmp(val, "1") == 0);
            return sql_bool_init(ctx, bval, false);
        }
        default:
        case SQL_TYPE_STRING: {
            return sql_string_init(ctx, val, (val[0] == '\0'));
        }
    }
}

static my_table_t *parse_table_def(aml_pool_t *pool, const char *json_str)
{
    ajson_t *root = ajson_parse_string(pool, json_str);
    if (ajson_is_error(root)) {
        printf("Error parsing table definition JSON.\n");
        return NULL;
    }

    my_table_t *table = aml_pool_zalloc(pool, sizeof(my_table_t));
    table->table_name = ajsono_scan_strd(pool, root, "name", NULL);

    ajson_t *cols = ajsono_get(root, "columns");
    if (!cols || ajson_is_error(cols) || ajson_type(cols) != array) {
        printf("Error: 'columns' must be an array.\n");
        return NULL;
    }

    size_t ncols = ajsona_count(cols);
    table->columns = aml_pool_alloc(pool, ncols * sizeof(sql_ctx_column_t));
    memset(table->columns, 0, ncols * sizeof(sql_ctx_column_t));
    table->num_columns = ncols;

    for (size_t i = 0; i < ncols; i++) {
        ajson_t *colobj = ajsona_scan(cols, (int)i);
        if (!colobj || ajson_is_error(colobj) || ajson_type(colobj) != object) {
            printf("Invalid column definition at index %zu.\n", i);
            return NULL;
        }

        const char *name = ajsono_scan_strd(pool, colobj, "name", NULL);
        const char *typestr = ajsono_scan_strd(pool, colobj, "type", "STRING");
        sql_data_type_t ctype = SQL_TYPE_STRING;
        if (typestr) {
            if (strcasecmp(typestr, "INT") == 0)
                ctype = SQL_TYPE_INT;
            else if (strcasecmp(typestr, "DOUBLE") == 0)
                ctype = SQL_TYPE_DOUBLE;
            else if (strcasecmp(typestr, "DATETIME") == 0)
                ctype = SQL_TYPE_DATETIME;
            else if (strcasecmp(typestr, "BOOL") == 0)
                ctype = SQL_TYPE_BOOL;
        }

        table->columns[i].name = (char*)name;
        table->columns[i].type = ctype;
        table->columns[i].func = my_col_getter;
    }

    return table;
}

static int parse_rows_for_table(my_table_t *table, const char *json_str)
{
    ajson_t *root = ajson_parse_string(g_pool, json_str);
    if (ajson_is_error(root)) {
        printf("Error: row JSON is invalid.\n");
        return -1;
    }
    if (ajson_type(root) != array) {
        printf("Expected an array of objects for rows.\n");
        return -1;
    }

    size_t nrows = ajsona_count(root);
    table->rows = aml_pool_alloc(g_pool, nrows * sizeof(my_row_t));
    memset(table->rows, 0, nrows * sizeof(my_row_t));
    table->num_rows = nrows;

    for (size_t r = 0; r < nrows; r++) {
        ajson_t *rowobj = ajsona_scan(root, (int)r);
        if (!rowobj || ajson_is_error(rowobj) || ajson_type(rowobj) != object) {
            printf("Row %zu is not a valid object.\n", r);
            return -1;
        }

        my_row_t *row = &table->rows[r];
        row->values = aml_pool_alloc(g_pool, table->num_columns * sizeof(char*));
        memset(row->values, 0, table->num_columns * sizeof(char*));

        for (size_t c = 0; c < table->num_columns; c++) {
            const char *col_name = table->columns[c].name;
            ajson_t *valnode = ajsono_get(rowobj, col_name);
            if (valnode && !ajson_is_error(valnode)) {
                ajson_type_t t = ajson_type(valnode);
                if (t == string) {
                    row->values[c] = ajson_to_strd(g_pool, valnode, "");
                } else if (t == number || t == decimal) {
                    double d = ajson_to_double(valnode, 0.0);
                    char tmp[64];
                    snprintf(tmp, sizeof(tmp), "%.0f", d);
                    row->values[c] = aml_pool_strdup(g_pool, tmp);
                } else if (t == bool_true || t == bool_false) {
                    bool b = ajson_to_bool(valnode, false);
                    row->values[c] = aml_pool_strdup(g_pool, b ? "true" : "false");
                } else {
                    row->values[c] = "";
                }
            } else {
                row->values[c] = "";
            }
        }
    }
    return 0;
}

static void evaluate_expression(sql_ctx_t *ctx, my_table_t *table, const char *expr_str)
{
    size_t token_count = 0;
    sql_token_t **tokens = sql_tokenize(ctx, expr_str, &token_count);
    if (!tokens || token_count == 0) {
        printf("Failed to tokenize.\n");
        return;
    }

    sql_ast_node_t *ast = sql_build_ast(ctx, tokens, token_count);
    if (!ast) {
        printf("AST build failed.\n");
        return;
    }

    sql_node_t *expr_node = sql_convert_ast_to_node(ctx, ast);
    if (!expr_node) {
        printf("Failed to convert AST to execution node.\n");
        return;
    }

    apply_type_conversions(ctx, expr_node);
    sql_simplify_func_tree(ctx, expr_node);
    sql_simplify_logical_expressions(expr_node);

    printf("=== Results for Expression: '%s' ===\n", expr_str);
    for (size_t r = 0; r < table->num_rows; r++) {
        ctx->row = &table->rows[r];

        bool match = false;
        sql_node_t *result = sql_eval(ctx, expr_node);

        if (result && result->data_type == SQL_TYPE_BOOL && !result->is_null) {
            match = result->value.bool_value;
        }

        if (match) {
            printf("Row %zu MATCH => ", r);
            for (size_t c = 0; c < table->num_columns; c++) {
                printf("%s=%s ", table->columns[c].name, table->rows[r].values[c] ? table->rows[r].values[c] : "NULL");
            }
            printf("\n");
        }
    }
}

int main(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <table_def.json> <rows.json> \"<expression>\"\n", argv[0]);
        return 1;
    }

    const char *table_def_file = argv[1];
    const char *rows_file      = argv[2];
    const char *expr_stmt      = argv[3];

    FILE *fp = fopen(table_def_file, "rb");
    if (!fp) { perror("fopen table_def"); return 1; }
    fseek(fp, 0, SEEK_END);
    long fsize = ftell(fp);
    rewind(fp);
    char *table_def_json = malloc(fsize+1);
    fread(table_def_json, 1, fsize, fp);
    table_def_json[fsize] = '\0';
    fclose(fp);

    fp = fopen(rows_file, "rb");
    if (!fp) { perror("fopen rows"); return 1; }
    fseek(fp, 0, SEEK_END);
    fsize = ftell(fp);
    rewind(fp);
    char *rows_json = malloc(fsize+1);
    fread(rows_json, 1, fsize, fp);
    rows_json[fsize] = '\0';
    fclose(fp);

    g_pool = aml_pool_init(1024*1024);

    my_table_t *table = parse_table_def(g_pool, table_def_json);
    if (!table) {
        printf("Failed to parse table def.\n");
        return 1;
    }

    if (parse_rows_for_table(table, rows_json) != 0) {
        printf("Failed to parse rows.\n");
        return 1;
    }

    free(table_def_json);
    free(rows_json);

    sql_ctx_t *context = aml_pool_zalloc(g_pool, sizeof(sql_ctx_t));
    context->pool = g_pool;
    context->columns = table->columns;
    context->column_count = table->num_columns;

    register_ctx(context);

    evaluate_expression(context, table, expr_stmt);

    size_t nerr = 0;
    char **errs = sql_ctx_get_errors(context, &nerr);
    for (size_t i = 0; i < nerr; i++) {
        printf("Error: %s\n", errs[i]);
    }

    aml_pool_destroy(g_pool);
    return 0;
}
