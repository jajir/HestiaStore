package org.hestiastore.index.segmentindex.core.executorregistry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.segmentindex.core.metrics.IndexExecutorRuntimeAccess;

/**
 * Owns executor lifecycle for SegmentIndex subsystems.
 */
public interface ExecutorRegistry extends CloseableResource {

    /**
     * Creates a builder for executor registry instances.
     *
     * @return executor registry builder
     */
    static ExecutorRegistryBuilder builder() {
        return new ExecutorRegistryBuilder();
    }

    /**
     * Returns shared segment maintenance executor.
     *
     * @return segment maintenance executor service
     * @throws IllegalStateException when registry has already been closed
     */
    ExecutorService getStableSegmentMaintenanceExecutor();

    /**
     * Returns shared index-maintenance executor.
     *
     * @return index-maintenance executor service
     * @throws IllegalStateException when registry has already been closed
     */
    ExecutorService getIndexMaintenanceExecutor();

    /**
     * Returns shared split-maintenance executor.
     *
     * @return split-maintenance executor service
     * @throws IllegalStateException when registry has already been closed
     */
    ExecutorService getSplitMaintenanceExecutor();

    /**
     * Returns shared split-policy scheduler.
     *
     * @return split-policy scheduler
     * @throws IllegalStateException when registry has already been closed
     */
    ScheduledExecutorService getSplitPolicyScheduler();

    /**
     * Returns shared registry-maintenance executor.
     *
     * @return registry-maintenance executor service
     * @throws IllegalStateException when registry has already been closed
     */
    ExecutorService getRegistryMaintenanceExecutor();

    /**
     * Captures current executor runtime metrics.
     *
     * @return executor runtime snapshot
     * @throws IllegalStateException when registry has already been closed
     */
    IndexExecutorRuntimeAccess runtimeSnapshot();
}
