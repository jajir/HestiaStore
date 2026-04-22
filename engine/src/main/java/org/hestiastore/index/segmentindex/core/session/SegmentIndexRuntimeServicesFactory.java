package org.hestiastore.index.segmentindex.core.session;

import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.segmentindex.core.control.IndexRuntimeControlPlane;
import org.hestiastore.index.segmentindex.core.control.SegmentRuntimeLimitApplier;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.core.maintenance.SegmentIndexMaintenanceAccess;
import org.hestiastore.index.segmentindex.core.metrics.SegmentIndexMetricsSnapshots;
import org.hestiastore.index.segmentindex.core.routing.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.routing.SegmentIndexRuntimeSplits;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorage;
import org.hestiastore.index.segmentindex.wal.WalRuntime;

/**
 * Builds runtime services that sit on top of core storage and split
 * infrastructure.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexRuntimeServicesFactory<K, V> {

    private final SegmentIndexRuntimeInputs<K, V> request;
    private final SegmentIndexCoreStorage<K, V> coreStorage;
    private final SegmentIndexRuntimeSplits<K, V> splitState;

    SegmentIndexRuntimeServicesFactory(
            final SegmentIndexRuntimeInputs<K, V> request,
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final SegmentIndexRuntimeSplits<K, V> splitState) {
        this.request = Vldtn.requireNonNull(request, "request");
        this.coreStorage = Vldtn.requireNonNull(coreStorage, "coreStorage");
        this.splitState = Vldtn.requireNonNull(splitState, "splitState");
    }

    SegmentIndexRuntimeServices<K, V> create(
            final WalRuntime<K, V> walRuntime) {
        final WalRuntime<K, V> validatedWalRuntime = Vldtn.requireNonNull(
                walRuntime, "walRuntime");
        final IndexWalCoordinator<K, V> walCoordinator = createWalCoordinator(
                validatedWalRuntime);
        final SegmentIndexOperationAccess<K, V> operationAccess =
                createOperationAccess(walCoordinator);
        final SegmentIndexMaintenanceAccess<K, V> maintenanceAccess =
                createMaintenanceAccess(walCoordinator);
        final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier =
                createRuntimeLimitApplier();
        final Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier =
                createMetricsSnapshotSupplier(validatedWalRuntime);
        final IndexControlPlane controlPlane = createControlPlane(
                metricsSnapshotSupplier, runtimeLimitApplier);
        return new SegmentIndexRuntimeServices<>(walCoordinator,
                operationAccess, maintenanceAccess,
                runtimeLimitApplier, metricsSnapshotSupplier, controlPlane);
    }

    private IndexWalCoordinator<K, V> createWalCoordinator(
            final WalRuntime<K, V> walRuntime) {
        return new IndexWalCoordinator<>(request.logger, request.conf,
                walRuntime, coreStorage.retryPolicy(), () -> { },
                this::flushStableStorage, request.stateSupplier,
                request.failureHandler, request.lastAppliedWalLsn);
    }

    private void flushStableStorage() {
        splitState.stableSegmentCoordinator().flushSegments(true);
        coreStorage.keyToSegmentMap().flushIfDirty();
    }

    private SegmentIndexOperationAccess<K, V> createOperationAccess(
            final IndexWalCoordinator<K, V> walCoordinator) {
        return SegmentIndexOperationAccess.create(
                request.valueTypeDescriptor,
                request.stats,
                splitState.directSegmentCoordinator(),
                Vldtn.requireNonNull(walCoordinator, "walCoordinator"),
                coreStorage.retryPolicy());
    }

    private SegmentIndexMaintenanceAccess<K, V> createMaintenanceAccess(
            final IndexWalCoordinator<K, V> walCoordinator) {
        return SegmentIndexMaintenanceAccess.create(
                coreStorage.keyToSegmentMap(),
                splitState.backgroundSplitCoordinator(),
                splitState.backgroundSplitPolicyLoop(),
                splitState.stableSegmentCoordinator(),
                Vldtn.requireNonNull(walCoordinator, "walCoordinator"));
    }

    private SegmentRuntimeLimitApplier<K, V> createRuntimeLimitApplier() {
        return new SegmentRuntimeLimitApplier<>(coreStorage.segmentRegistry(),
                coreStorage.segmentRegistry().runtime());
    }

    private Supplier<SegmentIndexMetricsSnapshot> createMetricsSnapshotSupplier(
            final WalRuntime<K, V> walRuntime) {
        return SegmentIndexMetricsSnapshots.create(
                request.conf, coreStorage.keyToSegmentMap(),
                coreStorage.segmentRegistry(),
                splitState.backgroundSplitCoordinator(),
                request.executorRegistry,
                coreStorage.runtimeTuningState(), walRuntime,
                request.stats, request.compactRequestHighWaterMark,
                request.flushRequestHighWaterMark, request.lastAppliedWalLsn,
                request.stateSupplier);
    }

    private IndexControlPlane createControlPlane(
            final Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier,
            final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier) {
        return new IndexRuntimeControlPlane(request.conf,
                coreStorage.runtimeTuningState(),
                request.stateSupplier,
                Vldtn.requireNonNull(metricsSnapshotSupplier,
                        "metricsSnapshotSupplier"),
                Vldtn.requireNonNull(runtimeLimitApplier,
                        "runtimeLimitApplier")::apply,
                splitState.backgroundSplitPolicyLoop()::scheduleScan);
    }
}
