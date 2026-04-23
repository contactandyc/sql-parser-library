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

#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_node.h"
#include "sql-parser-library/sql_ast.h"
#include "sql-parser-library/sql_tokenizer.h"
#include "sql-parser-library/sql_binder.h"
#include "sql-parser-library/date_utils.h"

#define MAX_PATH_LEN 1024

typedef struct my_table_s {
    char *table_name;
    sql_ctx_column_t *columns;
    size_t num_columns;

    ajson_t **rows;
    size_t num_rows;
} my_table_t;

static aml_pool_t *g_pool = NULL;

// --- DYNAMIC COLUMN GETTER FOR JSON ---
static sql_node_t *my_col_getter(sql_ctx_t *ctx, sql_node_t *f) {
    ajson_t **row_objs = (ajson_t **)ctx->row;
    sql_ctx_column_t *col_def = f->column;

    if (!col_def || !row_objs) {
        return sql_bool_init(ctx, false, true);
    }

    ajson_t *row_obj = row_objs[col_def->table_index];
    if (!row_obj) {
        return sql_bool_init(ctx, false, true);
    }

    const char *col_name = col_def->name;
    ajson_t *valnode = ajsono_get(row_obj, col_name);
    bool is_null = (!valnode || ajson_is_error(valnode) || ajson_is_null(valnode));

    switch (f->data_type) {
        case SQL_TYPE_INT: {
            int ival = is_null ? 0 : (int)ajson_to_double(valnode, 0.0);
            return sql_int_init(ctx, ival, is_null);
        }
        case SQL_TYPE_DOUBLE: {
            double dval = is_null ? 0.0 : ajson_to_double(valnode, 0.0);
            return sql_double_init(ctx, dval, is_null);
        }
        case SQL_TYPE_DATETIME: {
            if (is_null) return sql_datetime_init(ctx, 0, true);

            const char *strval = ajson_to_strd(ctx->pool, valnode, "");
            if(strchr(strval, '-') != NULL || strlen(strval) == 4) {
                time_t epoch;
                if(convert_string_to_datetime(&epoch, ctx->pool, strval)) {
                    return sql_datetime_init(ctx, epoch, false);
                }
                return sql_datetime_init(ctx, 0, true);
            } else {
                time_t epoch = (time_t)ajson_to_int64(valnode, 0);
                return sql_datetime_init(ctx, epoch, false);
            }
        }
        case SQL_TYPE_BOOL: {
            bool bval = is_null ? false : ajson_to_bool(valnode, false);
            return sql_bool_init(ctx, bval, is_null);
        }
        default:
        case SQL_TYPE_STRING: {
            if (is_null) return sql_string_init(ctx, NULL, true);
            const char *strval = ajson_to_strd(ctx->pool, valnode, "");
            return sql_string_init(ctx, strval, false);
        }
    }
}

// --- DYNAMIC CATALOG LOOKUP ---
static sql_ctx_column_t *expression_catalog_lookup(sql_ctx_t *ctx, const char *table_name, const char *column_name) {
    my_table_t *table = (my_table_t *)ctx->catalog_state;
    if (!table) return NULL;

    for (size_t i = 0; i < table->num_columns; i++) {
        if (strcasecmp(table->columns[i].name, column_name) == 0) {
            if (table_name && table->table_name && strcasecmp(table->table_name, table_name) != 0) {
                continue;
            }
            return &table->columns[i];
        }
    }
    return NULL;
}

static my_table_t *parse_table_object(ajson_t *table_obj) {
    if (!table_obj || ajson_is_error(table_obj) || ajson_type(table_obj) != object) {
        printf("Table definition is invalid.\n");
        return NULL;
    }

    my_table_t *table = aml_pool_zalloc(g_pool, sizeof(my_table_t));
    table->table_name = ajsono_scan_strd(g_pool, table_obj, "name", "my_table");

    ajson_t *cols = ajsono_get(table_obj, "columns");
    if (!cols || ajson_is_error(cols) || ajson_type(cols) != array) {
        table->columns = NULL;
        table->num_columns = 0;
    } else {
        size_t ncols = ajsona_count(cols);
        table->columns = aml_pool_alloc(g_pool, ncols * sizeof(sql_ctx_column_t));
        memset(table->columns, 0, ncols * sizeof(sql_ctx_column_t));
        table->num_columns = ncols;

        for (size_t i = 0; i < ncols; i++) {
            ajson_t *colobj = ajsona_scan(cols, (int)i);
            const char *name = ajsono_scan_strd(g_pool, colobj, "name", NULL);
            const char *typestr = ajsono_scan_strd(g_pool, colobj, "type", "STRING");

            sql_data_type_t ctype = SQL_TYPE_STRING;
            if (typestr) {
                if (strcasecmp(typestr, "INT") == 0) ctype = SQL_TYPE_INT;
                else if (strcasecmp(typestr, "DOUBLE") == 0) ctype = SQL_TYPE_DOUBLE;
                else if (strcasecmp(typestr, "DATETIME") == 0) ctype = SQL_TYPE_DATETIME;
                else if (strcasecmp(typestr, "BOOL") == 0) ctype = SQL_TYPE_BOOL;
            }

            table->columns[i].table_name = table->table_name;
            table->columns[i].name = (char *)name;
            table->columns[i].type = ctype;
            table->columns[i].func = my_col_getter;
            table->columns[i].table_index = 0;
        }
    }

    ajson_t *rows_array = ajsono_get(table_obj, "rows");
    if (!rows_array || ajson_is_error(rows_array) || ajson_type(rows_array) != array) {
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
        table->rows[r] = (rowobj && ajson_type(rowobj) == object) ? rowobj : NULL;
    }

    return table;
}

static void run_one_query(my_table_t *table, const char *sql, char **expected_ids, size_t num_expected) {
    sql_ctx_t *ctx = aml_pool_zalloc(g_pool, sizeof(sql_ctx_t));
    ctx->pool = g_pool;
    ctx->schema_lookup = expression_catalog_lookup;
    ctx->catalog_state = table;
    register_ctx(ctx);

    printf("  [TEST] %s", sql);

    size_t token_count = 0;
    sql_token_t **tokens = sql_tokenize(ctx, sql, &token_count);
    if (!tokens) {
        printf(" => FAILED (Tokenization)\n");
        return;
    }

    sql_ast_node_t *ast = build_ast(ctx, tokens, token_count);
    if (!ast) {
        printf(" => FAILED (AST Build)\n");
        return;
    }

    sql_node_t *expr_node = NULL;
    if (ast) {
        sql_bind_expression(ctx, ast);
        expr_node = convert_ast_to_node(ctx, ast);
        apply_type_conversions(ctx, expr_node);
        simplify_func_tree(ctx, expr_node);
        simplify_logical_expressions(expr_node);
    }

    int id_col_index = -1;
    for (size_t i = 0; i < table->num_columns; i++) {
        if (strcasecmp(table->columns[i].name, "id") == 0) {
            id_col_index = i;
            break;
        }
    }

    char **actual_ids = aml_pool_alloc(ctx->pool, table->num_rows * sizeof(char*));
    size_t actual_count = 0;
    ajson_t *current_row_set[1];

    for (size_t r = 0; r < table->num_rows; r++) {
        ajson_t *row_obj = table->rows[r];
        if (!row_obj) continue;

        current_row_set[0] = row_obj;
        ctx->row = current_row_set;

        bool matched = true;
        if (expr_node) {
            sql_node_t *result = sql_eval(ctx, expr_node);
            if (!result || result->data_type != SQL_TYPE_BOOL || !result->value.bool_value) {
                matched = false;
            }
        }
        if (matched) {
            if (id_col_index >= 0) {
                const char *col_name = table->columns[id_col_index].name;
                ajson_t *valnode = ajsono_get(row_obj, col_name);
                if (valnode && ajson_type(valnode) == string) {
                    actual_ids[actual_count++] = (char*)ajson_to_strd(ctx->pool, valnode, "");
                } else if (valnode && (ajson_type(valnode) == number || ajson_type(valnode) == decimal)) {
                    double d = ajson_to_double(valnode, 0.0);
                    char tmp[64];
                    snprintf(tmp, sizeof(tmp), "%.0f", d);
                    actual_ids[actual_count++] = aml_pool_strdup(ctx->pool, tmp);
                } else {
                    actual_ids[actual_count++] = (char*)"";
                }
            } else {
                char tmp[32];
                snprintf(tmp, sizeof(tmp), "ROW-%zu", r);
                actual_ids[actual_count++] = aml_pool_strdup(ctx->pool, tmp);
            }
        }
    }

    bool mismatch = (actual_count != num_expected);
    if (!mismatch) {
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

    if(mismatch) {
        printf("\n    => [FAILED] Expected: ");
        for(size_t i=0; i<num_expected; i++) printf("%s ", expected_ids[i]);
        printf("| Got: ");
        for(size_t i=0; i<actual_count; i++) printf("%s ", actual_ids[i]);
        printf("\n");
    } else {
        printf(" => [OK]\n");
    }
}

static void run_all_queries(my_table_t *table, ajson_t *queries_array) {
    size_t qcount = ajsona_count(queries_array);
    for (size_t i = 0; i < qcount; i++) {
        ajson_t *qobj = ajsona_scan(queries_array, (int)i);
        const char *sql = ajsono_scan_strd(g_pool, qobj, "sql", "");

        ajson_t *exp = ajsono_get(qobj, "expected");
        size_t nexp = 0;
        char **expected_list = NULL;

        if (exp && !ajson_is_error(exp)) {
            if (ajson_type(exp) == array) {
                nexp = ajsona_count(exp);
                expected_list = aml_pool_alloc(g_pool, nexp * sizeof(char*));
                for (size_t e = 0; e < nexp; e++) {
                    ajson_t *valnode = ajsona_scan(exp, (int)e);
                    expected_list[e] = (valnode && ajson_type(valnode) == string) ? ajson_to_strd(g_pool, valnode, "") : (char*)"";
                }
            } else if (ajson_type(exp) == string) {
                nexp = 1;
                expected_list = aml_pool_alloc(g_pool, sizeof(char*));
                expected_list[0] = ajson_to_strd(g_pool, exp, "");
            }
        }
        run_one_query(table, sql, expected_list, nexp);
    }
}

static void process_json_file(const char *json_file) {
    FILE *fp = fopen(json_file, "rb");
    if (!fp) return;

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

    printf("\nRunning: %s\n", json_file);

    ajson_t *table_obj = ajsono_get(root, "table");
    my_table_t *table = parse_table_object(table_obj);
    ajson_t *queries_array = ajsono_get(root, "queries");

    if (table && queries_array) {
        run_all_queries(table, queries_array);
    }

    aml_pool_destroy(g_pool);
}

static void process_directory(const char *dir_path) {
    DIR *dir = opendir(dir_path);
    if (!dir) return;

    struct dirent *entry;
    char path[MAX_PATH_LEN];

    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) continue;
        snprintf(path, sizeof(path), "%s/%s", dir_path, entry->d_name);

        struct stat path_stat;
        if (stat(path, &path_stat) == 0) {
            if (S_ISDIR(path_stat.st_mode)) process_directory(path);
            else if (S_ISREG(path_stat.st_mode) && strstr(entry->d_name, ".json")) process_json_file(path);
        }
    }
    closedir(dir);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <test.json | directory>\n", argv[0]);
        return 1;
    }

    struct stat path_stat;
    if (stat(argv[1], &path_stat) != 0) {
        perror("stat");
        return 1;
    }

    if (S_ISDIR(path_stat.st_mode)) process_directory(argv[1]);
    else process_json_file(argv[1]);

    return 0;
}