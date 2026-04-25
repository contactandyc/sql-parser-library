// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef _date_utils_H
#define _date_utils_H

#define _XOPEN_SOURCE 700
#include <time.h>
#include <stdbool.h>
#include "a-memory-library/aml_pool.h"

/**
 * Parses a timezone string and returns its standard abbreviation.
 */
const char *date_utils_get_timezone(const char *date_str);

/**
 * Calculates the integer offset (in seconds) for a given timezone string.
 */
int date_utils_get_timezone_offset(const char *timezone_part);

/**
 * Converts a standard SQL datetime string into a UNIX epoch timestamp.
 * The resulting epoch is stored in the provided result pointer. Any temporary
 * string allocations needed during parsing are handled by the provided arena pool.
 * Returns true if the raw string was parsed successfully, or false if the format is invalid.
 */
bool date_utils_convert_string_to_datetime(time_t *result, aml_pool_t *pool, const char *date_str);

/**
 * Converts a UNIX epoch timestamp back into an ISO-8601 UTC string.
 * The resulting string is allocated on the provided memory pool.
 */
char *date_utils_convert_epoch_to_iso_utc(aml_pool_t *pool, time_t epoch);

#endif
