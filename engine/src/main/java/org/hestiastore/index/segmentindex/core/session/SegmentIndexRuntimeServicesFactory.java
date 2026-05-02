package org.hestiastore.index.segmentindex.core.session;

import java.util.function.Supplier;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeConfiguration;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.core.control.RuntimeConfigurationImpl;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoringImpl;
import org.hestiastore.index.segmentindex.core.control.SegmentRuntimeLimitApplier;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.metrics.RuntimeMetricsCollector;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationAccess;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
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
    private final SegmentTopologyRuntime<K, V> topologyRuntime;

    SegmentIndexRuntimeServicesFactory(
            final SegmentIndexRuntimeInputs<K, V> request,
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final SegmentTopologyRuntime<K, V> topologyRuntime) {
        this.request = Vldtn.requireNonNull(request, "request");
        this.coreStorage = Vldtn.requireNonNull(coreStorage, "coreStorage");
        this.topologyRuntime = Vldtn.requireNonNull(topologyRuntime,
                "topologyRuntime");
    }

    SegmentIndexRuntimeServices<K, V> create(
            final WalRuntime<K, V> walRuntime) {
        final WalRuntime<K, V> validatedWalRuntime = Vldtn.requireNonNull(
                walRuntime, "walRuntime");
        final AtomicReference<Runnable> checkpointAction =
                new AtomicReference<>(() -> {
                });
        final MaintenanceService maintenance = createMaintenance(
                () -> checkpointAction.get().run());
        final IndexWalCoordinator<K, V> walCoordinator = createWalCoordinator(
                validatedWalRuntime, maintenance::flushAndWait);
        checkpointAction.set(walCoordinator::checkpoint);
        final SegmentIndexOperationAccess<K, V> operationAccess =
                createOperationAccess(walCoordinator);
        final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier =
                createRuntimeLimitApplier();
        final Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier =
                createMetricsSnapshotSupplier(validatedWalRuntime);
        final IndexRuntimeMonitoring runtimeMonitoring = createRuntimeMonitoring(
                metricsSnapshotSupplier);
        final RuntimeConfiguration runtimeConfiguration = createRuntimeConfiguration(
                runtimeLimitApplier);
        return new SegmentIndexRuntimeServices<>(walCoordinator,
                operationAccess, maintenance,
                runtimeLimitApplier, metricsSnapshotSupplier,
                runtimeMonitoring, runtimeConfiguration);
    }

    private IndexWalCoordinator<K, V> createWalCoordinator(
            final WalRuntime<K, V> walRuntime,
            final Runnable flushDurableStateAction) {
        return new IndexWalCoordinator<>(request.logger, request.conf,
                walRuntime, coreStorage.retryPolicy(), () -> { },
                Vldtn.requireNonNull(flushDurableStateAction,
                        "flushDurableStateAction"),
                request.stateSupplier, request.failureHandler,
                request.lastAppliedWalLsn);
    }

    private SegmentIndexOperationAccess<K, V> createOperationAccess(
            final IndexWalCoordinator<K, V> walCoordinator) {
        return SegmentIndexOperationAccess.create(
                request.valueTypeDescriptor,
                request.stats,
                topologyRuntime.segmentAccessService(),
                Vldtn.requireNonNull(walCoordinator, "walCoordinator"));
    }

    private MaintenanceService createMaintenance(
            final Runnable checkpointAction) {
        return MaintenanceService.<K, V>builder()
                .logger(request.logger)
                .keyToSegmentMap(coreStorage.keyToSegmentMap())
                .stableSegmentGateway(
                        StableSegmentOperationAccess.create(
                                coreStorage.segmentRegistry()))
                .splitService(topologyRuntime.splitService())
                .retryPolicy(coreStorage.retryPolicy())
                .stats(request.stats)
                .maintenanceExecutor(
                        request.executorRegistry.getIndexMaintenanceExecutor())
                .checkpointAction(checkpointAction)
                .build();
    }

    private SegmentRuntimeLimitApplier<K, V> createRuntimeLimitApplier() {
        return new SegmentRuntimeLimitApplier<>(coreStorage.segmentRegistry(),
                coreStorage.segmentRegistry().runtime());
    }

    private Supplier<SegmentIndexMetricsSnapshot> createMetricsSnapshotSupplier(
            final WalRuntime<K, V> walRuntime) {
        final RuntimeMetricsCollector runtimeMetricsCollector =
                RuntimeMetricsCollector.<K, V>builder()
                .withConf(request.conf)
                .withKeyToSegmentMap(coreStorage.keyToSegmentMap())
                .withSegmentRegistry(coreStorage.segmentRegistry())
                .withSplitSnapshotSupplier(() -> topologyRuntime.splitService()
                        .splitMetricsView()
                        .metricsSnapshot())
                .withExecutorRegistry(request.executorRegistry)
                .withRuntimeTuningState(coreStorage.runtimeTuningState())
                .withWalRuntime(walRuntime)
                .withStats(request.stats)
                .withCompactRequestHighWaterMark(
                        request.compactRequestHighWaterMark)
                .withFlushRequestHighWaterMark(
                        request.flushRequestHighWaterMark)
                .withLastAppliedWalLsn(request.lastAppliedWalLsn)
                .withStateSupplier(request.stateSupplier)
                .build();
        return runtimeMetricsCollector::metricsSnapshot;
    }

    private IndexRuntimeMonitoring createRuntimeMonitoring(
            final Supplier<SegmentIndexMetricsSnapshot> metricsSnapshotSupplier) {
        return new IndexRuntimeMonitoringImpl(request.conf,
                request.stateSupplier,
                Vldtn.requireNonNull(metricsSnapshotSupplier,
                        "metricsSnapshotSupplier"));
    }

    private RuntimeConfiguration createRuntimeConfiguration(
            final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier) {
        return new RuntimeConfigurationImpl(coreStorage.runtimeTuningState(),
                Vldtn.requireNonNull(runtimeLimitApplier,
                        "runtimeLimitApplier")::apply,
                topologyRuntime::requestFullSplitScan);
    }
}
