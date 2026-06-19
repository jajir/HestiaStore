package org.hestiastore.index.segmentindex.core.executorregistry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;

/**
 * Owns executor lifecycle for SegmentIndex subsystems.
 */
public final class ExecutorRegistry extends AbstractCloseableResource {

    private static final String MESSAGE_ALREADY_CLOSED = "ExecutorRegistry already closed";

    private final ExecutorTopology topology;
    private final ExecutorRuntimeMonitor runtimeMonitor;

    ExecutorRegistry(final ExecutorTopology topology,
            final ExecutorRuntimeMonitor runtimeMonitor) {
        this.topology = Vldtn.requireNonNull(topology, "topology");
        this.runtimeMonitor = Vldtn.requireNonNull(runtimeMonitor,
                "runtimeMonitor");
    }

    /**
     * Creates a builder for executor registry instances.
     *
     * @return executor registry builder
     */
    public static ExecutorRegistryBuilder builder() {
        return new ExecutorRegistryBuilder();
    }

    /**
     * Returns shared segment maintenance executor.
     *
     * @return segment maintenance executor service
     * @throws IllegalStateException when registry has already been closed
     */
    public ExecutorService getStableSegmentMaintenanceExecutor() {
        ensureOpen();
        return topology.stableSegmentMaintenanceExecutor();
    }

    /**
     * Returns shared index-maintenance executor.
     *
     * @return index-maintenance executor service
     * @throws IllegalStateException when registry has already been closed
     */
    public ExecutorService getIndexMaintenanceExecutor() {
        ensureOpen();
        return topology.indexMaintenanceExecutor();
    }

    /**
     * Returns shared split-maintenance executor.
     *
     * @return split-maintenance executor service
     * @throws IllegalStateException when registry has already been closed
     */
    public ExecutorService getSplitMaintenanceExecutor() {
        ensureOpen();
        return topology.splitMaintenanceExecutor();
    }

    /**
     * Returns shared split-policy scheduler.
     *
     * @return split-policy scheduler
     * @throws IllegalStateException when registry has already been closed
     */
    public ScheduledExecutorService getSplitPolicyScheduler() {
        ensureOpen();
        return topology.splitPolicyScheduler();
    }

    /**
     * Returns shared registry-maintenance executor.
     *
     * @return registry-maintenance executor service
     * @throws IllegalStateException when registry has already been closed
     */
    public ExecutorService getRegistryMaintenanceExecutor() {
        ensureOpen();
        return topology.registryMaintenanceExecutor();
    }

    /**
     * Captures current executor runtime stats.
     *
     * @return executor registry stats snapshot
     */
    public ExecutorRegistryStats statsSnapshot() {
        return runtimeMonitor.statsSnapshot();
    }

    private void ensureOpen() {
        if (wasClosed()) {
            throw new IllegalStateException(MESSAGE_ALREADY_CLOSED);
        }
    }

    @Override
    protected void doClose() {
        final RuntimeException failure = topology.shutdownExecutorsInCloseOrder();
        if (failure != null) {
            throw failure;
        }
    }

}
