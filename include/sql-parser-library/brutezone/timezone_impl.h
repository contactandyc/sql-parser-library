// SPDX-FileCopyrightText: 2024-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai — technical questions: contact Andy (above)
// SPDX-License-Identifier: Apache-2.0

#ifndef BRUTEZONE_TIMEZONE_IMPL_H
#define BRUTEZONE_TIMEZONE_IMPL_H

/*
 * This file is adapted from the brutezone project.
 * Original repository: https://github.com/gregjesl/brutezone
 * Licensed under the MIT License.
 * Copyright (c) 2018 Gregory Eslinger
 */

/* 2000-03-01 (mod 400 year, immediately after feb29 */
#define LEAPOCH (946684800LL + 86400 * (31 + 29))

#define DAYS_PER_400Y (365 * 400 + 97)
#define DAYS_PER_100Y (365 * 100 + 24)
#define DAYS_PER_4Y (365 * 4 + 1)

#ifdef __cplusplus
extern "C" {
#endif
long long year_to_secs(long long year, int *is_leap);
int month_to_secs(int month, int is_leap);
#ifdef __cplusplus
}
#endif

#endif
