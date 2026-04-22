package org.hestiastore.index.segmentindex.core.metrics;

/**
 * Stable read-only metrics view for executor groups owned by one registry.
 */
public interface IndexExecutorRuntimeAccess {

    IndexExecutorMetricsAccess getSplitPlanner();

    IndexExecutorMetricsAccess getSplitMaintenance();

    IndexExecutorMetricsAccess getStableSegmentMaintenance();
}
