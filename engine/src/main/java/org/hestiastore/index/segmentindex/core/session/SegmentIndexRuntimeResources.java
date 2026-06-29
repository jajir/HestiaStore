package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.SegmentIndexRuntimeState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.execution.MaintenanceStatsRecorder;
import org.hestiastore.index.segmentindex.core.execution.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.split.SplitStatsRecorder;

/**
 * Holds package-private session resources while bootstrap steps live outside
 * the session package.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexRuntimeResources<K, V>
        implements SegmentIndexRuntimeState {

    private final SegmentIndexStateMachine stateMachine =
            new SegmentIndexStateMachine();
    private final IndexOperationStatsRecorder operationStatsRecorder =
            new IndexOperationStatsRecorder();
    private final MaintenanceStatsRecorder maintenanceStatsRecorder =
            new MaintenanceStatsRecorder();
    private final SplitStatsRecorder splitStatsRecorder =
            new SplitStatsRecorder();
    private final SessionOperationGate operationGate =
            SessionOperationGate.create();

    private IndexDirectoryLock directoryLock;
    private ExecutorRegistry executorRegistry;
    private SegmentIndexRuntimeHandle runtimeHandle;

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
     * Installs the runtime ownership handle used by the close flow.
     *
     * @param runtimeHandle runtime ownership handle
     */
    public void setRuntimeHandle(final SegmentIndexRuntimeHandle runtimeHandle) {
        this.runtimeHandle = Vldtn.requireNonNull(runtimeHandle,
                "runtimeHandle");
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

    /**
     * Marks the session runtime as failed.
     *
     * @param failure runtime failure cause
     */
    @Override
    public void markRuntimeFailure(final RuntimeException failure) {
        stateMachine.markRuntimeFailure(failure);
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

    SegmentIndexRuntimeHandle runtimeHandle() {
        return Vldtn.requireNonNull(runtimeHandle, "runtimeHandle");
    }

    public SplitStatsRecorder splitStatsRecorder() {
        return splitStatsRecorder;
    }

    SegmentIndexStateMachine stateMachine() {
        return stateMachine;
    }

    SessionOperationGate operationGate() {
        return operationGate;
    }

}
