// SPDX-FileCopyrightText: 2024-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai — technical questions: contact Andy (above)
// SPDX-License-Identifier: Apache-2.0

#ifndef _sql_interval_H
#define _sql_interval_H

#include "sql-parser-library/sql_ctx.h"

typedef struct {
    int years;
    int months;
    int days;
    int hours;
    int minutes;
    int seconds;
    int microseconds;
} sql_interval_t;

sql_interval_t *sql_interval_parse(sql_ctx_t *context, const char *interval);

#endif /* _sql_interval_H */
