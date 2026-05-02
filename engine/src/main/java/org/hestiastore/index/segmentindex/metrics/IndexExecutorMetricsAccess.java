package org.hestiastore.index.segmentindex.metrics;

/**
 * Stable read-only metrics view for one observed executor.
 */
public interface IndexExecutorMetricsAccess {

    int getActiveThreadCount();

    int getQueueSize();

    int getQueueCapacity();

    long getCompletedTaskCount();

    long getRejectedTaskCount();

    long getCallerRunsCount();
}
