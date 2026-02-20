package org.hestiastore.monitoring.micrometer;

/**
 * Metric names exposed by Micrometer integration.
 */
final class HestiaStoreMetricNames {

    static final String OPS_GET_TOTAL = "hestiastore_ops_get_total";
    static final String OPS_PUT_TOTAL = "hestiastore_ops_put_total";
    static final String OPS_DELETE_TOTAL = "hestiastore_ops_delete_total";
    static final String REGISTRY_CACHE_HIT_TOTAL = "hestiastore_registry_cache_hit_total";
    static final String REGISTRY_CACHE_MISS_TOTAL = "hestiastore_registry_cache_miss_total";
    static final String REGISTRY_CACHE_LOAD_TOTAL = "hestiastore_registry_cache_load_total";
    static final String REGISTRY_CACHE_EVICTION_TOTAL = "hestiastore_registry_cache_eviction_total";
    static final String REGISTRY_CACHE_SIZE = "hestiastore_registry_cache_size";
    static final String REGISTRY_CACHE_LIMIT = "hestiastore_registry_cache_limit";
    static final String INDEX_UP = "hestiastore_index_up";

    private HestiaStoreMetricNames() {
    }
}
