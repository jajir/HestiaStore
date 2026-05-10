package org.hestiastore.index.segmentindex.core.session;

import java.util.concurrent.atomic.AtomicBoolean;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;

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
    private final SegmentIndexStartupCoordinator<K, V> startupCoordinator;
    private final AtomicBoolean startupCompleted = new AtomicBoolean();
    private final AtomicBoolean failedStartupCleanup = new AtomicBoolean();

    SegmentIndexSessionOwner(
            final SegmentIndexStateMachine stateMachine,
            final SegmentIndexRuntime<K, V> runtime,
            final IndexCloseCoordinator<K, V> closeCoordinator,
            final SegmentIndexStartupCoordinator<K, V> startupCoordinator) {
        this.stateMachine = Vldtn.requireNonNull(stateMachine,
                "stateMachine");
        this.runtime = Vldtn.requireNonNull(runtime, "runtime");
        this.closeCoordinator = Vldtn.requireNonNull(closeCoordinator,
                "closeCoordinator");
        this.startupCoordinator = Vldtn.requireNonNull(startupCoordinator,
                "startupCoordinator");
    }

    SegmentIndexState getState() {
        return stateMachine.getState();
    }

    void ensureOperational() {
        stateMachine.ensureOperational();
    }

    void runMaintenanceOperation(final Runnable action) {
        ensureOperational();
        Vldtn.requireNonNull(action, "action").run();
        runtime.invalidateSegmentIterators();
    }

    void close() {
        if (failedStartupCleanup.get()) {
            closeCoordinator.closeAfterFailedStartup();
            return;
        }
        closeCoordinator.close();
    }

    void completeStartup() {
        if (!startupCompleted.compareAndSet(false, true)) {
            return;
        }
        startupCoordinator.completeStartup();
    }

    void prepareFailedStartupCleanup(final Throwable failure) {
        Vldtn.requireNonNull(failure, "failure");
        failedStartupCleanup.set(true);
    }

    SegmentIndexMetricsSnapshot metricsSnapshot() {
        return runtime.metricsSnapshot();
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
