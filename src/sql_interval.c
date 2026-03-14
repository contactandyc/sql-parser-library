// SPDX-FileCopyrightText: 2024-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai — technical questions: contact Andy (above)
// SPDX-License-Identifier: Apache-2.0

#include <ctype.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include "sql-parser-library/sql_ctx.h"
#include "sql-parser-library/sql_interval.h"

static int parse_integer(const char **str) {
    int value = 0;
    while (isdigit(**str)) {
        value = value * 10 + (**str - '0');
        (*str)++;
    }
    return value;
}

static void skip_whitespace(const char **str) {
    while (isspace(**str)) {
        (*str)++;
    }
}

static void parse_named_component(sql_interval_t *interval, const char *name, int value) {
    if (strcasecmp(name, "year") == 0 || strcasecmp(name, "years") == 0) {
        interval->years = value;
    } else if (strcasecmp(name, "month") == 0 || strcasecmp(name, "months") == 0) {
        interval->months = value;
    } else if (strcasecmp(name, "day") == 0 || strcasecmp(name, "days") == 0) {
        interval->days = value;
    } else if (strcasecmp(name, "hour") == 0 || strcasecmp(name, "hours") == 0) {
        interval->hours = value;
    } else if (strcasecmp(name, "minute") == 0 || strcasecmp(name, "minutes") == 0) {
        interval->minutes = value;
    } else if (strcasecmp(name, "second") == 0 || strcasecmp(name, "seconds") == 0) {
        interval->seconds = value;
    } else if (strcasecmp(name, "microsecond") == 0 || strcasecmp(name, "microseconds") == 0) {
        interval->microseconds = value;
    }
}

static void parse_complex_interval(sql_ctx_t *context, sql_interval_t *interval, const char *interval_str) {
    const char *ptr = interval_str;
    while (*ptr) {
        skip_whitespace(&ptr);
        int value = parse_integer(&ptr);
        skip_whitespace(&ptr);

        const char *start = ptr;
        while (isalpha(*ptr)) {
            ptr++;
        }
        size_t name_len = ptr - start;

        char *name = (char *)aml_pool_alloc(context->pool, name_len + 1);
        strncpy(name, start, name_len);
        name[name_len] = '\0';

        parse_named_component(interval, name, value);
    }
}

static void parse_iso8601_interval(sql_ctx_t *context, sql_interval_t *interval, const char *interval_str) {
    const char *ptr = interval_str;

    if (*ptr == 'P') {
        ptr++;
    } else {
        sql_ctx_error(context, "Invalid ISO-8601 interval format: Missing 'P'");
        return;
    }

    int is_time_section = 0;

    while (*ptr) {
        if (*ptr == 'T') {
            is_time_section = 1;
            ptr++;
            continue;
        }

        int value = parse_integer(&ptr);

        if (!is_time_section) {
            if (*ptr == 'Y') {
                interval->years = value;
                ptr++;
            } else if (*ptr == 'M') {
                interval->months = value;
                ptr++;
            } else if (*ptr == 'W') {
                interval->days = value * 7;
                ptr++;
            } else if (*ptr == 'D') {
                interval->days = value;
                ptr++;
            } else {
                sql_ctx_error(context, "Invalid ISO-8601 interval format: Unexpected '%c'", *ptr);
                return;
            }
        } else {
            if (*ptr == 'H') {
                interval->hours = value;
                ptr++;
            } else if (*ptr == 'M') {
                interval->minutes = value;
                ptr++;
            } else if (*ptr == 'S') {
                interval->seconds = value;
                ptr++;
            } else {
                sql_ctx_error(context, "Invalid ISO-8601 interval format: Unexpected '%c'", *ptr);
                return;
            }
        }
    }
}

sql_interval_t *sql_interval_parse(sql_ctx_t *context, const char *interval) {
    sql_interval_t *result = (sql_interval_t *)aml_pool_zalloc(context->pool, sizeof(sql_interval_t));

    // Determine whether to parse as complex or ISO-8601 format
    if (interval[0] == 'P') {
        parse_iso8601_interval(context, result, interval);
    } else {
        parse_complex_interval(context, result, interval);
    }

    return result;
}
