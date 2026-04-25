// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <dirent.h>
#include <sys/stat.h>

#include "a-json-library/ajson.h"
#include "a-memory-library/aml_pool.h"

// Include your existing SQL-related headers
#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_node.h"
#include "sql-parser-library/sql_ast.h"
#include "sql-parser-library/sql_tokenizer.h"
#include "sql-parser-library/date_utils.h"

#define MAX_PATH_LEN 1024

//--------------------------------------------------------------
// We'll store each row as ajson_t* (JSON objects)
//--------------------------------------------------------------
typedef struct my_table_s {
    char *table_name;
    sql_ctx_column_t *columns;  // optional schema array
    size_t num_columns;

    ajson_t **rows;  // array of JSON objects, each row is a JSON object
    size_t num_rows;
} my_table_t;

// A global memory pool
static aml_pool_t *g_pool = NULL;

//--------------------------------------------------------------
// Dynamically lookup the column name in the JSON row
//--------------------------------------------------------------
static sql_node_t *my_col_getter(sql_ctx_t *ctx, sql_node_t *f)
{
    // 'ctx->row' will be an ajson_t* representing the row object
    ajson_t *row_obj = (ajson_t *)ctx->row;
    const char *col_name = f->token; // e.g. "source_integration"

    // Look up valnode in the row's JSON
    ajson_t *valnode = ajsono_get(row_obj, col_name);
    if (!valnode || ajson_is_error(valnode)) {
        // fallback to empty
        return sql_string_init(ctx, "", true);
    }

    // Convert based on f->data_type
    switch (f->data_type) {
        case SQL_TYPE_INT: {
            int ival = (int)ajson_to_double(valnode, 0.0);
            return sql_int_init(ctx, ival, false);
        }
        case SQL_TYPE_DOUBLE: {
            double dval = ajson_to_double(valnode, 0.0);
            return sql_double_init(ctx, dval, false);
        }
        case SQL_TYPE_DATETIME: {
            const char *strval = ajson_to_strd(ctx->pool, valnode, "");
            if(strchr(strval, '-') != NULL || strlen(strval)==4) {
                // Assume it's a date string
                time_t epoch;
                if(date_utils_convert_string_to_datetime(&epoch, ctx->pool, strval)) {
                    return sql_datetime_init(ctx, epoch, false);
                }
                return sql_datetime_init(ctx, 0, true);
            }
            else {
                time_t epoch = (time_t)ajson_to_int64(valnode, 0);
                bool isnull = (epoch == 0);
                return sql_datetime_init(ctx, epoch, isnull);
            }
        }
        case SQL_TYPE_BOOL: {
            bool bval = ajson_to_bool(valnode, false);
            return sql_bool_init(ctx, bval, false);
        }
        default:
        case SQL_TYPE_STRING: {
            const char *strval = ajson_to_strd(ctx->pool, valnode, "");
            bool isnull = (strval[0] == '\0');
            return sql_string_init(ctx, strval, isnull);
        }
    }
}

//--------------------------------------------------------------
// Parse table definition object: columns, and a "rows" array
//--------------------------------------------------------------
static my_table_t *parse_table_object(ajson_t *table_obj)
{
    if (!table_obj || ajson_is_error(table_obj) || ajson_type(table_obj) != object) {
        printf("Table definition is invalid.\n");
        return NULL;
    }

    my_table_t *table = aml_pool_zalloc(g_pool, sizeof(my_table_t));

    table->table_name = ajsono_scan_strd(g_pool, table_obj, "name", "my_table");

    // parse columns array (optional if you want typed references)
    ajson_t *cols = ajsono_get(table_obj, "columns");
    if (!cols || ajson_is_error(cols) || ajson_type(cols) != array) {
        // We can handle no columns, or treat as 0
        printf("No 'columns' array or invalid, treating as 0 columns.\n");
        table->columns = NULL;
        table->num_columns = 0;
    } else {
        size_t ncols = ajsona_count(cols);
        table->columns = aml_pool_alloc(g_pool, ncols * sizeof(sql_ctx_column_t));
        memset(table->columns, 0, ncols * sizeof(sql_ctx_column_t));
        table->num_columns = ncols;

        for (size_t i = 0; i < ncols; i++) {
            ajson_t *colobj = ajsona_scan(cols, (int)i);
            if (!colobj || ajson_is_error(colobj) || ajson_type(colobj) != object) {
                printf("Invalid column definition at index %zu.\n", i);
                return NULL;
            }

            const char *name = ajsono_scan_strd(g_pool, colobj, "name", NULL);
            const char *typestr = ajsono_scan_strd(g_pool, colobj, "type", "STRING");
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
            table->columns[i].name = (char *)name;
            table->columns[i].type = ctype;
            table->columns[i].func = my_col_getter;
        }
    }

    // parse rows as an array of JSON objects
    ajson_t *rows_array = ajsono_get(table_obj, "rows");
    if (!rows_array || ajson_is_error(rows_array) || ajson_type(rows_array) != array) {
        printf("No valid 'rows' array, treating as zero rows.\n");
        table->rows = NULL;
        table->num_rows = 0;
        return table;
    }

    size_t nrows = ajsona_count(rows_array);
    table->rows = aml_pool_alloc(g_pool, nrows * sizeof(ajson_t *));
    memset(table->rows, 0, nrows * sizeof(ajson_t *));
    table->num_rows = nrows;

    for (size_t r = 0; r < nrows; r++) {
        ajson_t *rowobj = ajsona_scan(rows_array, (int)r);
        if (!rowobj || ajson_is_error(rowobj) || ajson_type(rowobj) != object) {
            printf("Row %zu is not a valid object.\n", r);
            // partial parse
            table->rows[r] = NULL;
        } else {
            table->rows[r] = rowobj;
        }
    }

    return table;
}


//--------------------------------------------------------------
// Evaluate a single expression, gather matching "id" column, compare
//--------------------------------------------------------------
static void debug_one_query(my_table_t *table, const char *sql,
                            char **expected_ids, size_t num_expected)
{
    // Build sql_ctx_t
    sql_ctx_t *ctx = aml_pool_zalloc(g_pool, sizeof(sql_ctx_t));
    ctx->pool = g_pool;
    ctx->columns = table->columns;
    ctx->column_count = table->num_columns;
    register_ctx(ctx);

    // tokenize and parse
    size_t token_count = 0;
    sql_token_t **tokens = sql_tokenize(ctx, sql, &token_count);
    if (!tokens) {
        printf("Failed to tokenize.\n");
        return;
    }
    sql_token_print(tokens, token_count);
    sql_ast_node_t *ast = sql_build_ast(ctx, tokens, token_count);
    if (!ast) {
        printf("AST build failed.\n");
        return;
    }

    sql_node_t *expr_node = NULL;
    if (ast) {
        sql_print_ast(ast, 0);
        expr_node = sql_convert_ast_to_node(ctx, ast);
        sql_print_node(ctx, expr_node, 0);
        apply_type_conversions(ctx, expr_node);
        sql_print_node(ctx, expr_node, 0);
        sql_simplify_func_tree(ctx, expr_node);
        sql_print_node(ctx, expr_node, 0);
        sql_simplify_logical_expressions(expr_node);
        sql_print_node(ctx, expr_node, 0);
    }

    // We'll find the "id" column name if we want to compare row IDs
    int id_col_index = -1;
    for (size_t i = 0; i < table->num_columns; i++) {
        if (strcasecmp(table->columns[i].name, "id") == 0) {
            id_col_index = i;
            break;
        }
    }

    // Gather actual matched IDs in a temporary array
    char **actual_ids = aml_pool_alloc(ctx->pool, table->num_rows * sizeof(char*));
    size_t actual_count = 0;

    // Evaluate row by row
    for (size_t r = 0; r < table->num_rows; r++) {
        ajson_t *row_obj = table->rows[r];
        if (!row_obj) continue; // skip invalid

        // set context->row to the JSON object
        ctx->row = row_obj;

        bool matched = true;
        if (expr_node) {
            sql_node_t *result = sql_eval(ctx, expr_node);
            sql_print_node(ctx, result, 0);
            if (!result || result->data_type != SQL_TYPE_BOOL || !result->value.bool_value) {
                matched = false;
            }
        }
        if (matched) {
            // If we want to gather the row's "id" field:
            if (id_col_index >= 0) {
                // Instead of reading row->values, we do a dynamic JSON lookup:
                const char *col_name = table->columns[id_col_index].name;
                ajson_t *valnode = ajsono_get(row_obj, col_name);
                if (valnode && ajson_type(valnode) == string) {
                    actual_ids[actual_count++] = (char*)ajson_to_strd(ctx->pool, valnode, "");
                } else if (valnode && (ajson_type(valnode) == number || ajson_type(valnode) == decimal)) {
                    // convert number to string
                    double d = ajson_to_double(valnode, 0.0);
                    char tmp[64];
                    snprintf(tmp, sizeof(tmp), "%.0f", d);
                    actual_ids[actual_count++] = aml_pool_strdup(ctx->pool, tmp);
                } else {
                    // fallback
                    actual_ids[actual_count++] = (char*)"";
                }
            } else {
                // no "id" column -> label them "ROW-x"
                char tmp[32];
                snprintf(tmp, sizeof(tmp), "ROW-%zu", r);
                actual_ids[actual_count++] = aml_pool_strdup(ctx->pool, tmp);
            }
        }
    }

    // Compare actual vs. expected
    bool mismatch = false;
    if (actual_count != num_expected) {
        mismatch = true;
    } else {
        for (size_t i = 0; i < actual_count; i++) {
            bool found = false;
            for(size_t j = 0; j < num_expected; j++) {
                if (strcasecmp(actual_ids[i], expected_ids[j]) == 0) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                mismatch = true;
                break;
            }
        }
    }

    // print results
    printf("\nExpected %zu => ", num_expected);
    for (size_t i = 0; i < num_expected; i++) {
        printf("%s ", expected_ids[i]);
    }
    printf("\nGot %zu => ", actual_count);
    for (size_t i = 0; i < actual_count; i++) {
        printf("%s ", actual_ids[i]);
    }
    printf("\n\n");
}


//--------------------------------------------------------------
// Evaluate a single expression, gather matching "id" column, compare
//--------------------------------------------------------------
static void run_one_query(my_table_t *table, const char *sql,
                          char **expected_ids, size_t num_expected, bool detailed)
{
    // Build sql_ctx_t
    sql_ctx_t *ctx = aml_pool_zalloc(g_pool, sizeof(sql_ctx_t));
    ctx->pool = g_pool;
    ctx->columns = table->columns;
    ctx->column_count = table->num_columns;
    register_ctx(ctx);

    printf("%s", sql);

    // tokenize and parse
    size_t token_count = 0;
    sql_token_t **tokens = sql_tokenize(ctx, sql, &token_count);
    if (!tokens) {
        printf(" => FAILED (Failed to tokenize.)\n");
        return;
    }
    sql_ast_node_t *ast = sql_build_ast(ctx, tokens, token_count);
    if (!ast) {
        printf(" => FAILED (AST build failed.)\n");
        return;
    }

    sql_node_t *expr_node = NULL;
    if (ast) {
        expr_node = sql_convert_ast_to_node(ctx, ast);
        apply_type_conversions(ctx, expr_node);
        sql_simplify_func_tree(ctx, expr_node);
        sql_simplify_logical_expressions(expr_node);
    }

    // We'll find the "id" column name if we want to compare row IDs
    int id_col_index = -1;
    for (size_t i = 0; i < table->num_columns; i++) {
        if (strcasecmp(table->columns[i].name, "id") == 0) {
            id_col_index = i;
            break;
        }
    }

    // Gather actual matched IDs in a temporary array
    char **actual_ids = aml_pool_alloc(ctx->pool, table->num_rows * sizeof(char*));
    size_t actual_count = 0;

    // Evaluate row by row
    for (size_t r = 0; r < table->num_rows; r++) {
        ajson_t *row_obj = table->rows[r];
        if (!row_obj) continue; // skip invalid

        // set context->row to the JSON object
        ctx->row = row_obj;

        bool matched = true;
        if (expr_node) {
            sql_node_t *result = sql_eval(ctx, expr_node);
            if (!result || result->data_type != SQL_TYPE_BOOL || !result->value.bool_value) {
                matched = false;
            }
        }
        if (matched) {
            // If we want to gather the row's "id" field:
            if (id_col_index >= 0) {
                // Instead of reading row->values, we do a dynamic JSON lookup:
                const char *col_name = table->columns[id_col_index].name;
                ajson_t *valnode = ajsono_get(row_obj, col_name);
                if (valnode && ajson_type(valnode) == string) {
                    actual_ids[actual_count++] = (char*)ajson_to_strd(ctx->pool, valnode, "");
                } else if (valnode && (ajson_type(valnode) == number || ajson_type(valnode) == decimal)) {
                    // convert number to string
                    double d = ajson_to_double(valnode, 0.0);
                    char tmp[64];
                    snprintf(tmp, sizeof(tmp), "%.0f", d);
                    actual_ids[actual_count++] = aml_pool_strdup(ctx->pool, tmp);
                } else {
                    // fallback
                    actual_ids[actual_count++] = (char*)"";
                }
            } else {
                // no "id" column -> label them "ROW-x"
                char tmp[32];
                snprintf(tmp, sizeof(tmp), "ROW-%zu", r);
                actual_ids[actual_count++] = aml_pool_strdup(ctx->pool, tmp);
            }
        }
    }

    // Compare actual vs. expected
    bool mismatch = false;
    if (actual_count != num_expected) {
        mismatch = true;
    } else {
        for (size_t i = 0; i < actual_count; i++) {
            bool found = false;
            for(size_t j = 0; j < num_expected; j++) {
                if (strcasecmp(actual_ids[i], expected_ids[j]) == 0) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                mismatch = true;
                break;
            }
        }
    }

    // print results
    if(mismatch) {
        printf(" => FAILED\n");
        if (detailed)
            debug_one_query(table, sql, expected_ids, num_expected);
    } else {
        printf(" => OK\n");
        if (detailed)
            debug_one_query(table, sql, expected_ids, num_expected);
    }
}

//--------------------------------------------------------------
// Run all queries in the "queries" array
//--------------------------------------------------------------
static void run_all_queries(my_table_t *table, ajson_t *queries_array, int argc, char **argv)
{
    size_t qcount = ajsona_count(queries_array);
    for (size_t i = 0; i < qcount; i++) {
        ajson_t *qobj = ajsona_scan(queries_array, (int)i);
        if (!qobj || ajson_is_error(qobj) || ajson_type(qobj) != object) {
            printf("Query #%zu is invalid.\n", i);
            continue;
        }

        const char *sql = ajsono_scan_strd(g_pool, qobj, "sql", "");
        if (!sql || !*sql) {
            printf("Query #%zu has no 'sql'.\n", i);
            continue;
        }

        bool found = false;
        for( int i=0; i<argc; i++ ) {
            if(!strstr(sql, argv[i])) {
                found = true;
                break;
            }
        }
        if(found)
            continue;

        // parse the "expected" array
        ajson_t *exp = ajsono_get(qobj, "expected");
        size_t nexp = 0;
        char **expected_list = NULL;
        if (exp && !ajson_is_error(exp)) {
            if (ajson_type(exp) == array) {
                nexp = ajsona_count(exp);
                expected_list = aml_pool_alloc(g_pool, nexp * sizeof(char*));
                for (size_t e = 0; e < nexp; e++) {
                    ajson_t *valnode = ajsona_scan(exp, (int)e);
                    if (valnode && ajson_type(valnode) == string) {
                        expected_list[e] = ajson_to_strd(g_pool, valnode, "");
                    } else {
                        expected_list[e] = (char*)"";
                    }
                }
            } else if (ajson_type(exp) == string) {
                // single string means 1 expected ID
                nexp = 1;
                expected_list = aml_pool_alloc(g_pool, sizeof(char*));
                expected_list[0] = ajson_to_strd(g_pool, exp, "");
            }
        }

        run_one_query(table, sql, expected_list, nexp, argc > 0);
    }
}

// Function prototypes
static void process_json_file(const char *json_file, int argc, char **argv);
static void process_directory(const char *dir_path, int argc, char **argv);

//--------------------------------------------------------------
// Main
//--------------------------------------------------------------
int main(int argc, char **argv)
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <test.json | directory>\n", argv[0]);
        return 1;
    }

    struct stat path_stat;
    if (stat(argv[1], &path_stat) != 0) {
        perror("stat");
        return 1;
    }

    if (S_ISDIR(path_stat.st_mode)) {
        // If it's a directory, process all JSON files inside
        process_directory(argv[1], argc, argv);
    } else {
        // Otherwise, process a single file
        process_json_file(argv[1], argc, argv);
    }

    return 0;
}

//--------------------------------------------------------------
// Recursively process all JSON files in a directory
//--------------------------------------------------------------
static void process_directory(const char *dir_path, int argc, char **argv)
{
    DIR *dir = opendir(dir_path);
    if (!dir) {
        perror("opendir");
        return;
    }

    struct dirent *entry;
    char path[MAX_PATH_LEN];

    while ((entry = readdir(dir)) != NULL) {
        // Skip "." and ".."
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        snprintf(path, sizeof(path), "%s/%s", dir_path, entry->d_name);

        struct stat path_stat;
        if (stat(path, &path_stat) != 0) {
            perror("stat");
            continue;
        }

        if (S_ISDIR(path_stat.st_mode)) {
            // Recursively process subdirectories
            process_directory(path, argc, argv);
        } else if (S_ISREG(path_stat.st_mode) && strstr(entry->d_name, ".json")) {
            // Process only JSON files
            printf("\nProcessing JSON file: %s\n", path);
            process_json_file(path, argc, argv);
        }
    }

    closedir(dir);
}

//--------------------------------------------------------------
// Process a single JSON file
//--------------------------------------------------------------
static void process_json_file(const char *json_file, int argc, char **argv)
{
    FILE *fp = fopen(json_file, "rb");
    if (!fp) {
        perror("fopen");
        return;
    }

    fseek(fp, 0, SEEK_END);
    long fsz = ftell(fp);
    rewind(fp);
    char *buf = malloc(fsz + 1);
    fread(buf, 1, fsz, fp);
    buf[fsz] = '\0';
    fclose(fp);

    g_pool = aml_pool_init(1024 * 1024);

    ajson_t *root = ajson_parse_string(g_pool, buf);
    free(buf);

    if (ajson_is_error(root) || ajson_type(root) != object) {
        printf("Invalid JSON: %s\n", json_file);
        return;
    }

    ajson_t *table_obj = ajsono_get(root, "table");
    my_table_t *table = parse_table_object(table_obj);
    if (!table) {
        printf("Failed to parse table in %s\n", json_file);
        return;
    }

    ajson_t *queries_array = ajsono_get(root, "queries");
    if (!queries_array || ajson_is_error(queries_array) || ajson_type(queries_array) != array) {
        printf("No queries array in %s\n", json_file);
        return;
    }

    run_all_queries(table, queries_array, argc - 2, argv + 2);

    aml_pool_destroy(g_pool);
}
