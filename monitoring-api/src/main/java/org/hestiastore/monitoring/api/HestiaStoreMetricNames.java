package org.hestiastore.monitoring.api;

/**
 * Shared metric names used by monitoring integrations.
 */
public final class HestiaStoreMetricNames {

    public static final String OPS_GET_TOTAL = "hestiastore_ops_get_total";
    public static final String OPS_PUT_TOTAL = "hestiastore_ops_put_total";
    public static final String OPS_DELETE_TOTAL = "hestiastore_ops_delete_total";
    public static final String REGISTRY_CACHE_HIT_TOTAL = "hestiastore_registry_cache_hit_total";
    public static final String REGISTRY_CACHE_MISS_TOTAL = "hestiastore_registry_cache_miss_total";
    public static final String REGISTRY_CACHE_LOAD_TOTAL = "hestiastore_registry_cache_load_total";
    public static final String REGISTRY_CACHE_EVICTION_TOTAL = "hestiastore_registry_cache_eviction_total";
    public static final String REGISTRY_CACHE_SIZE = "hestiastore_registry_cache_size";
    public static final String REGISTRY_CACHE_LIMIT = "hestiastore_registry_cache_limit";
    public static final String INDEX_UP = "hestiastore_index_up";

    private HestiaStoreMetricNames() {
    }
}
