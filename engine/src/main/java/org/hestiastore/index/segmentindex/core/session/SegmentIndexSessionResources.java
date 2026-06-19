package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateView;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.split.SplitStatsRecorder;
import org.hestiastore.index.segmentindex.core.storage.WalRuntimeFailureHandler;

/**
 * Holds package-private session resources while bootstrap steps live outside
 * the session package.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexSessionResources<K, V>
        implements SegmentIndexStateView, WalRuntimeFailureHandler {

    private final SegmentIndexStateMachine stateMachine =
            new SegmentIndexStateMachine();
    private final IndexOperationStatsRecorder operationStatsRecorder =
            new IndexOperationStatsRecorder();
    private final MaintenanceStatsRecorder maintenanceStatsRecorder =
            new MaintenanceStatsRecorder();
    private final SplitStatsRecorder splitStatsRecorder =
            new SplitStatsRecorder();
    private final SegmentIndexOperationGate operationGate =
            SegmentIndexOperationGate.create();
    private final SegmentIndexTrackedOperationRunner trackedRunner =
            new SegmentIndexTrackedOperationRunner(stateMachine, operationGate);

    private IndexDirectoryLock directoryLock;
    private ExecutorRegistry executorRegistry;

    public void acquireDirectoryLock(final Directory directory) {
        directoryLock = new IndexDirectoryLock(directory);
    }

    public boolean wasStaleLockRecovered() {
        return directoryLock().wasStaleLockRecovered();
    }

    public void markReady() {
        stateMachine.markReady();
    }

    /**
     * Installs the executor registry owned by the live session.
     *
     * @param executorRegistry executor registry used by runtime and close flow
     */
    public void setExecutorRegistry(final ExecutorRegistry executorRegistry) {
        this.executorRegistry = Vldtn.requireNonNull(executorRegistry,
                "executorRegistry");
    }

    /**
     * Returns the current segment-index lifecycle state.
     *
     * @return current segment-index lifecycle state
     */
    @Override
    public SegmentIndexState currentState() {
        return stateMachine.getState();
    }

    public void markRuntimeFailure(final RuntimeException failure) {
        stateMachine.markRuntimeFailure(failure);
    }

    /**
     * Records a WAL runtime failure on the owning session.
     *
     * @param failure WAL runtime failure
     */
    @Override
    public void handleWalRuntimeFailure(final RuntimeException failure) {
        markRuntimeFailure(failure);
    }

    IndexDirectoryLock directoryLock() {
        return Vldtn.requireNonNull(directoryLock, "directoryLock");
    }

    public IndexOperationStatsRecorder operationStatsRecorder() {
        return operationStatsRecorder;
    }

    public MaintenanceStatsRecorder maintenanceStatsRecorder() {
        return maintenanceStatsRecorder;
    }

    ExecutorRegistry executorRegistry() {
        return Vldtn.requireNonNull(executorRegistry, "executorRegistry");
    }

    public SplitStatsRecorder splitStatsRecorder() {
        return splitStatsRecorder;
    }

    SegmentIndexStateMachine stateMachine() {
        return stateMachine;
    }

    SegmentIndexOperationGate operationGate() {
        return operationGate;
    }

    SegmentIndexTrackedOperationRunner trackedRunner() {
        return trackedRunner;
    }

}
