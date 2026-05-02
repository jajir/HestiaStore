package org.hestiastore.index.segmentindex.metrics;

/**
 * Stable read-only metrics view for executor groups owned by one registry.
 */
public interface IndexExecutorRuntimeAccess {

    IndexExecutorMetricsAccess getIndexMaintenance();

    IndexExecutorMetricsAccess getSplitMaintenance();

    IndexExecutorMetricsAccess getStableSegmentMaintenance();
}
