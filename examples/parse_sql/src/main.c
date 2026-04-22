// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "a-memory-library/aml_pool.h"
#include "a-json-library/ajson.h"
#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_tokenizer.h"
#include "sql-parser-library/sql_query.h"

// Helper to read the entire JSON file into memory
static char *read_file_content(const char *filename) {
    FILE *fp = fopen(filename, "rb");
    if (!fp) {
        perror("Failed to open file");
        return NULL;
    }
    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    rewind(fp);

    char *buffer = malloc(size + 1);
    if (buffer) {
        fread(buffer, 1, size, fp);
        buffer[size] = '\0';
    }
    fclose(fp);
    return buffer;
}

int main(int argc, char **argv) {
    // 1. Validate arguments
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <tests.json>\n", argv[0]);
        return 1;
    }

    const char *json_file = argv[1];
    char *json_content = read_file_content(json_file);
    if (!json_content) return 1;

    // 2. Initialize a pool for the JSON tree
    aml_pool_t *json_pool = aml_pool_init(1024 * 1024);
    ajson_t *root = ajson_parse_string(json_pool, json_content);
    free(json_content); // Free the raw buffer

    if (ajson_is_error(root) || ajson_type(root) != object) {
        fprintf(stderr, "Invalid JSON structure in %s\n", json_file);
        aml_pool_destroy(json_pool);
        return 1;
    }

    // 3. Extract the "queries" array
    ajson_t *queries_array = ajsono_get(root, "queries");
    if (!queries_array || ajson_is_error(queries_array) || ajson_type(queries_array) != array) {
        fprintf(stderr, "Expected a 'queries' array in the JSON.\n");
        aml_pool_destroy(json_pool);
        return 1;
    }

    size_t qcount = ajsona_count(queries_array);
    int passed = 0;
    int failed = 0;

    printf("Running %zu parser tests from %s...\n\n", qcount, json_file);

    // 4. Iterate through the tests
    for (size_t i = 0; i < qcount; i++) {
        ajson_t *qobj = ajsona_scan(queries_array, (int)i);

        // Extract the inputs
        const char *sql = ajsono_scan_strd(json_pool, qobj, "sql", "");
        const char *expected = ajsono_scan_strd(json_pool, qobj, "expected", "");

        if (!sql || !*sql) continue;

        // Use a dedicated pool for each test to prevent memory fragmentation
        aml_pool_t *test_pool = aml_pool_init(1024 * 1024);
        sql_ctx_t ctx = {0};
        ctx.pool = test_pool;
        register_ctx(&ctx);

        // Tokenize
        size_t token_count = 0;
        sql_token_t **tokens = sql_tokenize(&ctx, sql, &token_count);

        if (!tokens || token_count == 0) {
            printf("[FAIL] Query %zu: %s\n", i + 1, sql);
            printf("       Reason: Tokenization failed.\n\n");
            failed++;
            aml_pool_destroy(test_pool);
            continue;
        }

        // Parse AST
        sql_select_t *ast = sql_parse_query(&ctx, tokens, token_count);

        if (ast) {
            char *normalized_sql = sql_query_to_string(&ctx, ast);

            // Compare output
            if (strcmp(normalized_sql, expected) == 0) {
                printf("[PASS] %s\n", sql);
                passed++;
            } else {
                printf("[FAIL] %s\n", sql);
                printf("  Expected: %s\n", expected);
                printf("  Got:      %s\n\n", normalized_sql);
                failed++;
            }
        } else {
            printf("[FAIL] %s\n", sql);
            printf("  Reason: AST Parsing failed.\n");
            size_t nerr = 0;
            char **errs = sql_ctx_get_errors(&ctx, &nerr);
            for (size_t e = 0; e < nerr; e++) {
                printf("  Error: %s\n", errs[e]);
            }
            printf("\n");
            failed++;
        }

        aml_pool_destroy(test_pool);
    }

    // 5. Summary
    printf("\n=== TEST SUMMARY ===\n");
    printf("Passed: %d\n", passed);
    printf("Failed: %d\n", failed);

    aml_pool_destroy(json_pool);
    return (failed > 0) ? 1 : 0;
}
