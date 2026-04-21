package org.hestiastore.index.segmentindex.core;

import java.util.concurrent.atomic.AtomicBoolean;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.lifecycle.IndexCloseCoordinator;
import org.hestiastore.index.segmentindex.core.lifecycle.SegmentIndexStartupCoordinator;
import org.hestiastore.index.segmentindex.core.maintenance.SegmentIndexMaintenanceAccess;
import org.hestiastore.index.segmentindex.core.runtime.SegmentIndexRuntime;
import org.hestiastore.index.segmentindex.core.state.IndexState;
import org.hestiastore.index.segmentindex.core.state.IndexStateCoordinator;

/**
 * Owns lifecycle, state, and runtime collaborators behind one internal
 * boundary so {@link SegmentIndexImpl} can stay focused on API shape.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexCoreOwner<K, V> {

    private final IndexStateCoordinator<K, V> stateCoordinator;
    private final SegmentIndexRuntime<K, V> runtime;
    private final SegmentIndexMaintenanceAccess<K, V> maintenanceAccess;
    private final long indexBusyTimeoutMillis;
    private final IndexCloseCoordinator<K, V> closeCoordinator;
    private final SegmentIndexStartupCoordinator<K, V> startupCoordinator;
    private final AtomicBoolean startupCompleted = new AtomicBoolean();

    SegmentIndexCoreOwner(
            final IndexConfiguration<K, V> conf,
            final IndexStateCoordinator<K, V> stateCoordinator,
            final SegmentIndexRuntime<K, V> runtime,
            final SegmentIndexMaintenanceAccess<K, V> maintenanceAccess,
            final IndexCloseCoordinator<K, V> closeCoordinator,
            final SegmentIndexStartupCoordinator<K, V> startupCoordinator) {
        final IndexConfiguration<K, V> validatedConfiguration = Vldtn
                .requireNonNull(conf, "conf");
        this.stateCoordinator = Vldtn.requireNonNull(stateCoordinator,
                "stateCoordinator");
        this.runtime = Vldtn.requireNonNull(runtime, "runtime");
        this.maintenanceAccess = Vldtn.requireNonNull(maintenanceAccess,
                "maintenanceAccess");
        this.indexBusyTimeoutMillis = validatedConfiguration
                .getIndexBusyTimeoutMillis();
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
        maintenanceAccess.awaitSplitsIdle(indexBusyTimeoutMillis);
        Vldtn.requireNonNull(action, "action").run();
        maintenanceAccess.invalidateSegmentIterators();
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

    IndexControlPlane controlPlane() {
        return runtime.controlPlane();
    }

    IndexStateCoordinator<K, V> stateCoordinator() {
        return stateCoordinator;
    }

    SegmentIndexRuntime<K, V> runtime() {
        return runtime;
    }
}
