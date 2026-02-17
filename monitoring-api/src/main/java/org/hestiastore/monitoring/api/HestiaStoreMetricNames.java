package org.hestiastore.monitoring.api;

/**
 * Shared metric names used by monitoring integrations.
 */
public final class HestiaStoreMetricNames {

    public static final String OPS_GET_TOTAL = "hestiastore_ops_get_total";
    public static final String OPS_PUT_TOTAL = "hestiastore_ops_put_total";
    public static final String OPS_DELETE_TOTAL = "hestiastore_ops_delete_total";
    public static final String INDEX_UP = "hestiastore_index_up";

    private HestiaStoreMetricNames() {
    }
}
