package org.hestiastore.index.segmentindex.core.session;

import java.util.concurrent.atomic.AtomicBoolean;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeConfiguration;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.session.state.IndexState;
import org.hestiastore.index.segmentindex.core.session.state.IndexStateCoordinator;

/**
 * Owns lifecycle, state, and runtime collaborators behind one internal
 * boundary so {@link SegmentIndexImpl} can stay focused on API shape.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexSessionOwner<K, V> {

    private final IndexStateCoordinator<K, V> stateCoordinator;
    private final SegmentIndexRuntime<K, V> runtime;
    private final IndexCloseCoordinator<K, V> closeCoordinator;
    private final SegmentIndexStartupCoordinator<K, V> startupCoordinator;
    private final AtomicBoolean startupCompleted = new AtomicBoolean();

    SegmentIndexSessionOwner(
            final IndexStateCoordinator<K, V> stateCoordinator,
            final SegmentIndexRuntime<K, V> runtime,
            final IndexCloseCoordinator<K, V> closeCoordinator,
            final SegmentIndexStartupCoordinator<K, V> startupCoordinator) {
        this.stateCoordinator = Vldtn.requireNonNull(stateCoordinator,
                "stateCoordinator");
        this.runtime = Vldtn.requireNonNull(runtime, "runtime");
        this.closeCoordinator = Vldtn.requireNonNull(closeCoordinator,
                "closeCoordinator");
        this.startupCoordinator = Vldtn.requireNonNull(startupCoordinator,
                "startupCoordinator");
    }

    IndexState<K, V> getIndexState() {
        return stateCoordinator.getIndexState();
    }

    SegmentIndexState getState() {
        return stateCoordinator.getState();
    }

    void failWithError(final Throwable failure) {
        stateCoordinator.failWithError(failure);
    }

    void ensureOperational() {
        getIndexState().tryPerformOperation();
    }

    void runMaintenanceOperation(final Runnable action) {
        ensureOperational();
        Vldtn.requireNonNull(action, "action").run();
        runtime.invalidateSegmentIterators();
    }

    void close() {
        closeCoordinator.close();
    }

    void completeStartup(final Runnable consistencyCheckHook) {
        if (!startupCompleted.compareAndSet(false, true)) {
            return;
        }
        startupCoordinator.completeStartup(consistencyCheckHook);
    }

    SegmentIndexMetricsSnapshot metricsSnapshot() {
        return runtime.metricsSnapshot();
    }

    IndexRuntimeMonitoring runtimeMonitoring() {
        return runtime.runtimeMonitoring();
    }

    RuntimeConfiguration runtimeConfiguration() {
        return runtime.runtimeConfiguration();
    }

    IndexStateCoordinator<K, V> stateCoordinator() {
        return stateCoordinator;
    }

    SegmentIndexRuntime<K, V> runtime() {
        return runtime;
    }
}
