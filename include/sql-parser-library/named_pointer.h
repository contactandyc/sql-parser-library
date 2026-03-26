// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef _named_pointer_H
#define _named_pointer_H

#include "the-macro-library/macro_map.h"
#include "a-memory-library/aml_pool.h"

typedef struct {
    macro_map_t *name_map;
    macro_map_t *ptr_map;
} named_pointer_t;

void register_named_pointer(aml_pool_t *pool, named_pointer_t *np,
                            void *ptr, const char *name, const char *description);
const char *get_named_pointer_name(named_pointer_t *np, void *ptr);
const char *get_named_pointer_description(named_pointer_t *np, void *ptr);
void *get_named_pointer_pointer(named_pointer_t *np, const char *name);

#endif
