// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef _sql_named_pointer_H
#define _sql_named_pointer_H

#include "the-macro-library/macro_map.h"
#include "a-memory-library/aml_pool.h"

/**
 * A bi-directional lookup map for registering named C-function pointers.
 * Useful for mapping string identifiers (like "SUM") to actual C callbacks.
 */
typedef struct {
    macro_map_t *name_map;
    macro_map_t *ptr_map;
} sql_named_pointer_t;

/** Registers a new function pointer with a string name and description. */
void sql_register_named_pointer(aml_pool_t *pool, sql_named_pointer_t *np,
                                void *ptr, const char *name, const char *description);

/** Retrieves the registered name for a given function pointer. */
const char *sql_get_named_pointer_name(sql_named_pointer_t *np, void *ptr);

/** Retrieves the registered description for a given function pointer. */
const char *sql_get_named_pointer_description(sql_named_pointer_t *np, void *ptr);

/** Retrieves the raw function pointer for a given registered string name. */
void *sql_get_named_pointer_pointer(sql_named_pointer_t *np, const char *name);

#endif
