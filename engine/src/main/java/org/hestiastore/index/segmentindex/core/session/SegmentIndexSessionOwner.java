package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;

/**
 * Owns lifecycle, state, and runtime collaborators behind one internal
 * boundary so {@link SegmentIndexImpl} can stay focused on API shape.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexSessionOwner<K, V> {

    private final SegmentIndexStateMachine stateMachine;
    private final SegmentIndexRuntime<K, V> runtime;
    private final IndexCloseCoordinator<K, V> closeCoordinator;

    SegmentIndexSessionOwner(
            final SegmentIndexStateMachine stateMachine,
            final SegmentIndexRuntime<K, V> runtime,
            final IndexCloseCoordinator<K, V> closeCoordinator) {
        this.stateMachine = Vldtn.requireNonNull(stateMachine,
                "stateMachine");
        this.runtime = Vldtn.requireNonNull(runtime, "runtime");
        this.closeCoordinator = Vldtn.requireNonNull(closeCoordinator,
                "closeCoordinator");
    }

    void ensureOperational() {
        stateMachine.ensureOperational();
    }

    void invalidateSegmentIterators() {
        runtime.invalidateSegmentIterators();
    }

    void close() {
        closeCoordinator.close();
    }

    IndexRuntimeMonitoring runtimeMonitoring() {
        return runtime.runtimeMonitoring();
    }

    RuntimeTuning runtimeTuning() {
        return runtime.runtimeTuning();
    }

    SegmentIndexStateMachine stateMachine() {
        return stateMachine;
    }

    SegmentIndexRuntime<K, V> runtime() {
        return runtime;
    }
}
