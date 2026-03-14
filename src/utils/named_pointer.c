// SPDX-FileCopyrightText: 2024-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai — technical questions: contact Andy (above)
// SPDX-License-Identifier: Apache-2.0

#include "sql-parser-library/named_pointer.h"
#include "the-macro-library/macro_map.h"
#include <strings.h>

struct named_pointer_node_s;
typedef struct named_pointer_node_s named_pointer_node_t;

struct named_pointer_node_s {
    macro_map_t name_map;
    macro_map_t ptr_map;

    const char *name;
    void *ptr;

    const char *description;
};

static inline int named_name_insert_compare(const named_pointer_node_t *a,
                                            const named_pointer_node_t *b) {
    return strcasecmp(a->name, b->name);
}

static inline int named_name_find_compare(const char *name, const named_pointer_node_t *o) {
    return strcasecmp(name, o->name);
}

static inline int named_ptr_insert_compare(const named_pointer_node_t *a,
                                           const named_pointer_node_t *b) {
    if(a->ptr != b->ptr) {
        return a->ptr < b->ptr ? -1 : 1;
    }
    return 0;
}

static inline int named_ptr_find_compare(const void *ptr, const named_pointer_node_t *o) {
    if(ptr != o->ptr) {
        return ptr < o->ptr ? -1 : 1;
    }
    return 0;
}

static inline
macro_map_find_kv(named_find_name, char, named_pointer_node_t,
                  named_name_find_compare);

static inline
macro_map_insert(named_insert_name, named_pointer_node_t,
                 named_name_insert_compare);

static inline
macro_map_find_kv_with_field(named_find_ptr, ptr_map, void, named_pointer_node_t,
                             named_ptr_find_compare);

static inline
macro_map_insert_with_field(named_insert_ptr, ptr_map, named_pointer_node_t,
                            named_ptr_insert_compare);

void register_named_pointer(aml_pool_t *pool, named_pointer_t *np,
                            void *ptr, const char *name, const char *description) {
    if (!np || !name || !ptr) return;

    named_pointer_node_t *p = named_find_ptr(np->ptr_map, ptr);
    if (p) {
        // This pointer is already registered - should not happen
        abort();
    }

    p = (named_pointer_node_t *)aml_pool_zalloc(pool, sizeof(named_pointer_node_t));
    p->name = name;
    p->description = description;
    p->ptr = ptr;

    // Insert into the map
    named_insert_name(&np->name_map, p);
    named_insert_ptr(&np->ptr_map, p);
}

const char *get_named_pointer_name(named_pointer_t *np, void *ptr) {
    if (!np || !ptr) return NULL;

    named_pointer_node_t *r = named_find_ptr(np->ptr_map, ptr);
    if (!r) return NULL;
    return r->name;
}

const char *get_named_pointer_description(named_pointer_t *np, void *ptr) {
    if (!np || !ptr) return NULL;

    named_pointer_node_t *r = named_find_ptr(np->ptr_map, ptr);
    if (!r) return NULL;
    return r->name;
}

void *get_named_pointer_pointer(named_pointer_t *np, const char *name) {
    if (!np || !name) return NULL;

    named_pointer_node_t *r = named_find_name(np->name_map, name);
    if (!r) return NULL;
    return r->ptr;
}
