package org.hestiastore.index.segmentindex.core.executorregistry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.metrics.IndexExecutorRuntimeAccess;

final class ExecutorRegistryImpl extends AbstractCloseableResource
        implements ExecutorRegistry {

    private static final String MESSAGE_ALREADY_CLOSED = "ExecutorRegistry already closed";

    private final ExecutorTopology topology;
    private final ExecutorRuntimeMonitor runtimeMonitor;

    ExecutorRegistryImpl(final ExecutorTopology topology,
            final ExecutorRuntimeMonitor runtimeMonitor) {
        this.topology = Vldtn.requireNonNull(topology, "topology");
        this.runtimeMonitor = Vldtn.requireNonNull(runtimeMonitor,
                "runtimeMonitor");
    }

    @Override
    public ExecutorService getStableSegmentMaintenanceExecutor() {
        ensureOpen();
        return topology.stableSegmentMaintenanceExecutor();
    }

    @Override
    public ExecutorService getIndexMaintenanceExecutor() {
        ensureOpen();
        return topology.indexMaintenanceExecutor();
    }

    @Override
    public ExecutorService getSplitMaintenanceExecutor() {
        ensureOpen();
        return topology.splitMaintenanceExecutor();
    }

    @Override
    public ScheduledExecutorService getSplitPolicyScheduler() {
        ensureOpen();
        return topology.splitPolicyScheduler();
    }

    @Override
    public ExecutorService getRegistryMaintenanceExecutor() {
        ensureOpen();
        return topology.registryMaintenanceExecutor();
    }

    @Override
    public IndexExecutorRuntimeAccess runtimeSnapshot() {
        return runtimeMonitor.runtimeSnapshot();
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
