package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateView;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.split.SplitStatsRecorder;

/**
 * Holds package-private session resources while bootstrap steps live outside
 * the session package.
 * <p>
 * A future bootstrap model can replace this holder with
 * SegmentIndexBootstrapRequest and SegmentIndexBootstrapState builders that
 * enforce resource initialization order and availability.
 * </p>
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexSessionResources<K, V>
        implements SegmentIndexStateView {

    private IndexDirectoryLock directoryLock;
    private SegmentIndexSessionInfrastructure<K, V> sessionInfrastructure;
    private ExecutorRegistry executorRegistry;
    private SegmentIndexRuntime<K, V> runtime;

    public void acquireDirectoryLock(final Directory directory) {
        directoryLock = new IndexDirectoryLock(directory);
    }

    public boolean wasStaleLockRecovered() {
        return directoryLock().wasStaleLockRecovered();
    }

    public void recoverFromWal() {
        runtime().recoverFromWal();
    }

    public void cleanupOrphanedSegmentDirectories() {
        runtime().cleanupOrphanedSegmentDirectories();
    }

    public void markReady() {
        sessionInfrastructure().markReady();
    }

    public void runStartupConsistencyCheck() {
        runtime().runStartupConsistencyCheck();
    }

    public void requestFullSplitScan() {
        runtime().requestFullSplitScan();
    }

    /**
     * Installs the immutable session infrastructure created by the bootstrap
     * step.
     *
     * @param sessionInfrastructure state, stats, gate, and tracking resources
     */
    public void setSessionInfrastructure(
            final SegmentIndexSessionInfrastructure<K, V> sessionInfrastructure) {
        this.sessionInfrastructure = Vldtn.requireNonNull(
                sessionInfrastructure, "sessionInfrastructure");
    }

    public void setRuntime(final SegmentIndexRuntime<K, V> runtime,
            final ExecutorRegistry executorRegistry) {
        this.executorRegistry = Vldtn.requireNonNull(executorRegistry,
                "executorRegistry");
        this.runtime = Vldtn.requireNonNull(runtime, "runtime");
    }

    /**
     * Returns the current segment-index lifecycle state.
     *
     * @return current segment-index lifecycle state
     */
    @Override
    public SegmentIndexState currentState() {
        return sessionInfrastructure().currentState();
    }

    public void markRuntimeFailure(final RuntimeException failure) {
        sessionInfrastructure().markRuntimeFailure(failure);
    }

    public void closeRuntimeAfterFailedInitialization() {
        if (runtime != null) {
            runtime.closeAfterFailedInitialization();
        }
    }

    IndexDirectoryLock directoryLock() {
        return Vldtn.requireNonNull(directoryLock, "directoryLock");
    }

    SegmentIndexSessionInfrastructure<K, V> sessionInfrastructure() {
        return Vldtn.requireNonNull(sessionInfrastructure,
                "sessionInfrastructure");
    }

    public IndexOperationStatsRecorder operationStatsRecorder() {
        return sessionInfrastructure().operationStatsRecorder();
    }

    public MaintenanceStatsRecorder maintenanceStatsRecorder() {
        return sessionInfrastructure().maintenanceStatsRecorder();
    }

    ExecutorRegistry executorRegistry() {
        return Vldtn.requireNonNull(executorRegistry, "executorRegistry");
    }

    public SplitStatsRecorder splitStatsRecorder() {
        return sessionInfrastructure().splitStatsRecorder();
    }

    SegmentIndexRuntime<K, V> runtime() {
        return Vldtn.requireNonNull(runtime, "runtime");
    }

}
