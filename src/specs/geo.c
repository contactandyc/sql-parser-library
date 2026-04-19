// SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "sql-parser-library/sql_ctx.h"
#include <math.h>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

#define EARTH_RADIUS_MILES 3958.8
#define TO_RADIANS (M_PI / 180.0)

static sql_node_t *sql_func_geo_distance(sql_ctx_t *ctx, sql_node_t *f) {
    // Evaluate all 4 parameters: lat1, lon1, lat2, lon2
    sql_node_t *lat1_node = sql_eval(ctx, f->parameters[0]);
    sql_node_t *lon1_node = sql_eval(ctx, f->parameters[1]);
    sql_node_t *lat2_node = sql_eval(ctx, f->parameters[2]);
    sql_node_t *lon2_node = sql_eval(ctx, f->parameters[3]);

    // If any coordinate is NULL, the distance is NULL
    if (!lat1_node || lat1_node->is_null || !lon1_node || lon1_node->is_null ||
        !lat2_node || lat2_node->is_null || !lon2_node || lon2_node->is_null) {
        return sql_string_init(ctx, NULL, true);
        // return sql_double_init(ctx, 0.0, true);
    }

    // Convert degrees to radians
    double lat1 = lat1_node->value.double_value * TO_RADIANS;
    double lon1 = lon1_node->value.double_value * TO_RADIANS;
    double lat2 = lat2_node->value.double_value * TO_RADIANS;
    double lon2 = lon2_node->value.double_value * TO_RADIANS;

    // Haversine formula
    double dlat = lat2 - lat1;
    double dlon = lon2 - lon1;

    double a = sin(dlat / 2.0) * sin(dlat / 2.0) +
               cos(lat1) * cos(lat2) *
               sin(dlon / 2.0) * sin(dlon / 2.0);

    double c = 2.0 * atan2(sqrt(a), sqrt(1.0 - a));
    double distance = EARTH_RADIUS_MILES * c;
    // printf( "%0.3lf %0.3lf %0.3lf %0.3lf => %0.3f\n", lat1, lon1, lat2, lon2, distance );

    return sql_double_init(ctx, distance, false);
}

static sql_ctx_spec_update_t *update_geo_distance_spec(sql_ctx_t *ctx, sql_ctx_spec_t *spec, sql_node_t *f) {
    if (f->num_parameters != 4) {
        sql_ctx_error(ctx, "GEO_DISTANCE requires exactly 4 parameters: lat1, lon1, lat2, lon2.");
        return NULL;
    }

    sql_ctx_spec_update_t *update = (sql_ctx_spec_update_t *)aml_pool_zalloc(ctx->pool, sizeof(sql_ctx_spec_update_t));
    update->num_parameters = 4;
    update->parameters = f->parameters;
    update->expected_data_types = (sql_data_type_t *)aml_pool_alloc(ctx->pool, 4 * sizeof(sql_data_type_t));

    // Force all inputs to resolve as DOUBLE
    for (size_t i = 0; i < 4; i++) {
        update->expected_data_types[i] = SQL_TYPE_DOUBLE;
    }

    update->implementation = sql_func_geo_distance;
    update->return_type = SQL_TYPE_DOUBLE;

    return update;
}

sql_ctx_spec_t geo_distance_spec = {
    .name = "GEO_DISTANCE",
    .description = "Calculates the distance in miles between two lat/lon points.",
    .update = update_geo_distance_spec
};

void sql_register_geo(sql_ctx_t *ctx) {
    sql_ctx_register_spec(ctx, &geo_distance_spec);
    sql_ctx_register_callback(ctx, sql_func_geo_distance, "geo_distance", "Calculates distance in miles.");
}
