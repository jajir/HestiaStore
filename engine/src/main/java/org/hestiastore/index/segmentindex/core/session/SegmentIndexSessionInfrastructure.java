package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateView;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.split.SplitStatsRecorder;

/**
 * Immutable session infrastructure created before runtime collaborators are
 * assembled.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SegmentIndexSessionInfrastructure<K, V>
        implements SegmentIndexStateView {

    private final SegmentIndexStateMachine stateMachine;
    private final IndexOperationStatsRecorder operationStatsRecorder;
    private final MaintenanceStatsRecorder maintenanceStatsRecorder;
    private final SplitStatsRecorder splitStatsRecorder;
    private final SegmentIndexOperationGate operationGate;
    private final SegmentIndexTrackedOperationRunner<K, V> trackedRunner;

    private SegmentIndexSessionInfrastructure(
            final SegmentIndexStateMachine stateMachine,
            final IndexOperationStatsRecorder operationStatsRecorder,
            final MaintenanceStatsRecorder maintenanceStatsRecorder,
            final SplitStatsRecorder splitStatsRecorder,
            final SegmentIndexOperationGate operationGate,
            final SegmentIndexTrackedOperationRunner<K, V> trackedRunner) {
        this.stateMachine = stateMachine;
        this.operationStatsRecorder = operationStatsRecorder;
        this.maintenanceStatsRecorder = maintenanceStatsRecorder;
        this.splitStatsRecorder = splitStatsRecorder;
        this.operationGate = operationGate;
        this.trackedRunner = trackedRunner;
    }

    /**
     * Creates the state machine, stats recorders, operation gate, and tracked
     * runner for one index session.
     *
     * @param <K> key type
     * @param <V> value type
     * @return session infrastructure
     */
    public static <K, V> SegmentIndexSessionInfrastructure<K, V> create() {
        final SegmentIndexStateMachine stateMachine =
                new SegmentIndexStateMachine();
        final SegmentIndexOperationGate operationGate =
                SegmentIndexOperationGate.create();
        return new SegmentIndexSessionInfrastructure<>(
                stateMachine,
                new IndexOperationStatsRecorder(),
                new MaintenanceStatsRecorder(),
                new SplitStatsRecorder(),
                operationGate,
                new SegmentIndexTrackedOperationRunner<>(stateMachine,
                        operationGate));
    }

    /** {@inheritDoc} */
    @Override
    public SegmentIndexState currentState() {
        return stateMachine.getState();
    }

    void markReady() {
        stateMachine.markReady();
    }

    void markRuntimeFailure(final RuntimeException failure) {
        stateMachine.markRuntimeFailure(failure);
    }

    SegmentIndexStateMachine stateMachine() {
        return stateMachine;
    }

    IndexOperationStatsRecorder operationStatsRecorder() {
        return operationStatsRecorder;
    }

    MaintenanceStatsRecorder maintenanceStatsRecorder() {
        return maintenanceStatsRecorder;
    }

    SplitStatsRecorder splitStatsRecorder() {
        return splitStatsRecorder;
    }

    SegmentIndexOperationGate operationGate() {
        return operationGate;
    }

    SegmentIndexTrackedOperationRunner<K, V> trackedRunner() {
        return trackedRunner;
    }
}
