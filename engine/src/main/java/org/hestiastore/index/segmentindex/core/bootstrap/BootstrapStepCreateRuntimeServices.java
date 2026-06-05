package org.hestiastore.index.segmentindex.core.bootstrap;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationStorage;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningConfigurationMapper;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningServiceImpl;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningSnapshot;
import org.hestiastore.index.segmentindex.configuration.tuning.SegmentRuntimeLimitApplier;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceService;
import org.hestiastore.index.segmentindex.core.operations.SegmentIndexOperationAccess;
import org.hestiastore.index.segmentindex.core.segmentlease.SegmentLeaseService;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexRuntimeServices;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;
import org.hestiastore.index.segmentindex.core.session.SegmentTopologyRuntimeAccess;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationAccess;
import org.hestiastore.index.segmentindex.core.storage.IndexWalCoordinator;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorage;
import org.hestiastore.index.segmentindex.metrics.RuntimeMetricsCollector;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoringImpl;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentindex.wal.WalStats;

/**
 * Creates WAL coordination, maintenance, metrics, monitoring, and tuning
 * services for the runtime.
 */
final class BootstrapStepCreateRuntimeServices<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private final SegmentIndexSessionResources<K, V> sessionResources;

    BootstrapStepCreateRuntimeServices(
            final SegmentIndexSessionResources<K, V> sessionResources) {
        this.sessionResources = Vldtn.requireNonNull(sessionResources,
                "sessionResources");
    }

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        final SegmentIndexCoreStorage<K, V> coreStorage =
                state.getCoreStorage();
        final SegmentTopologyRuntimeAccess<K, V> topologyRuntime =
                state.getRuntimeTopologyRuntime();
        final SegmentLeaseService<K, V> segmentLeaseService =
                state.getRuntimeSegmentLeaseService();
        final SplitService splitService = state.getRuntimeSplitService();
        final AtomicReference<Runnable> checkpointAction =
                new AtomicReference<>(() -> {
                });
        final MaintenanceService maintenance = newMaintenance(coreStorage,
                splitService, state, () -> checkpointAction.get().run());
        final IndexWalCoordinator<K, V> walCoordinator =
                newWalCoordinator(state, coreStorage, maintenance);
        checkpointAction.set(walCoordinator::checkpoint);
        final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier =
                new SegmentRuntimeLimitApplier<>(coreStorage.segmentRegistry(),
                        coreStorage.segmentRegistry().runtime(),
                        coreStorage.chunkStoreCache());
        final RuntimeMetricsCollector runtimeMetricsCollector =
                newMetricsCollector(state, coreStorage, splitService);
        final IndexRuntimeMonitoring runtimeMonitoring =
                new IndexRuntimeMonitoringImpl(state.getConfiguration(),
                        sessionResources::currentState,
                        runtimeMetricsCollector);
        final RuntimeTuning runtimeTuning = newRuntimeTuning(request, state,
                coreStorage, topologyRuntime, runtimeLimitApplier);
        state.setRuntimeServices(new SegmentIndexRuntimeServices<>(
                walCoordinator,
                newOperationAccess(state, segmentLeaseService, walCoordinator),
                maintenance, runtimeLimitApplier, runtimeMetricsCollector,
                runtimeMonitoring, runtimeTuning));
    }

    private MaintenanceService newMaintenance(
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final SplitService splitService,
            final SegmentIndexBootstrapState<K, V> state,
            final Runnable checkpointAction) {
        return MaintenanceService.<K, V>builder()
                .keyToSegmentMap(coreStorage.keyToSegmentMap())
                .stableSegmentGateway(StableSegmentOperationAccess.create(
                        coreStorage.segmentRegistry()))
                .splitService(splitService)
                .retryPolicy(coreStorage.retryPolicy())
                .statsRecorder(sessionResources.maintenanceStatsRecorder())
                .maintenanceExecutor(
                        state.getExecutorRegistry()
                                .getIndexMaintenanceExecutor())
                .checkpointAction(checkpointAction)
                .build();
    }

    private IndexWalCoordinator<K, V> newWalCoordinator(
            final SegmentIndexBootstrapState<K, V> state,
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final MaintenanceService maintenance) {
        if (!state.getConfiguration().wal().isEnabled()) {
            return IndexWalCoordinator.disabled();
        }
        return IndexWalCoordinator.create(state.getConfiguration(),
                state.getRuntimeWalRuntime(), coreStorage.retryPolicy(), () -> { },
                maintenance::flushAndWait, sessionResources::currentState,
                sessionResources::markRuntimeFailure,
                state.lastAppliedWalLsn());
    }

    private SegmentIndexOperationAccess<K, V> newOperationAccess(
            final SegmentIndexBootstrapState<K, V> state,
            final SegmentLeaseService<K, V> segmentLeaseService,
            final IndexWalCoordinator<K, V> walCoordinator) {
        return SegmentIndexOperationAccess.create(
                state.getValueTypeDescriptor(),
                sessionResources.operationStatsRecorder(),
                segmentLeaseService,
                walCoordinator);
    }

    private RuntimeMetricsCollector newMetricsCollector(
            final SegmentIndexBootstrapState<K, V> state,
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final SplitService splitService) {
        return RuntimeMetricsCollector.<K, V>builder()
                .withConf(state.getConfiguration())
                .withKeyToSegmentMap(coreStorage.keyToSegmentMap())
                .withSegmentRegistry(coreStorage.segmentRegistry())
                .withSplitStatsSupplier(() -> splitService.splitStatsView()
                        .statsSnapshot())
                .withExecutorRegistry(state.getExecutorRegistry())
                .withRuntimeTuningState(coreStorage.runtimeTuningState())
                .withChunkStoreCache(coreStorage.chunkStoreCache())
                .withWalStatsSupplier(walStatsSupplier(state))
                .withIndexOperationStatsSupplier(sessionResources
                        .operationStatsRecorder()::statsSnapshot)
                .withMaintenanceStatsSupplier(sessionResources
                        .maintenanceStatsRecorder()::statsSnapshot)
                .withCompactRequestHighWaterMark(
                        state.compactRequestHighWaterMark())
                .withFlushRequestHighWaterMark(
                        state.flushRequestHighWaterMark())
                .withLastAppliedWalLsn(state.lastAppliedWalLsn())
                .withStateSupplier(sessionResources::currentState)
                .build();
    }

    private Supplier<WalStats> walStatsSupplier(
            final SegmentIndexBootstrapState<K, V> state) {
        if (!state.getConfiguration().wal().isEnabled()) {
            return WalStats::empty;
        }
        final WalRuntime<K, V> walRuntime = state.getRuntimeWalRuntime();
        return walRuntime::statsSnapshot;
    }

    private RuntimeTuning newRuntimeTuning(
            final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state,
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final SegmentTopologyRuntimeAccess<K, V> topologyRuntime,
            final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier) {
        final Directory directory = request.getDirectory();
        final EffectiveIndexConfiguration<K, V> configuration =
                state.getConfiguration();
        return new RuntimeTuningServiceImpl(coreStorage.runtimeTuningState(),
                runtimeLimitApplier::apply,
                topologyRuntime::requestFullSplitScan,
                snapshot -> persistRuntimeTuning(directory, configuration,
                        snapshot));
    }

    private void persistRuntimeTuning(final Directory directory,
            final EffectiveIndexConfiguration<K, V> configuration,
            final RuntimeTuningSnapshot snapshot) {
        new IndexConfigurationStorage<K, V>(directory)
                .save(RuntimeTuningConfigurationMapper.apply(configuration,
                        snapshot));
    }
}
