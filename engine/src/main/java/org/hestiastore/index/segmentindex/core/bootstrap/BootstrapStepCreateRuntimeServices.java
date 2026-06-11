package org.hestiastore.index.segmentindex.core.bootstrap;

import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexMaintenanceConfiguration;
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
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.wal.WalMonitoringView;

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
        final SegmentTopologyRuntimeAccess<K, V> topologyRuntime =
                state.getRuntimeTopologyRuntime();
        final SegmentLeaseService<K, V> segmentLeaseService =
                state.getRuntimeSegmentLeaseService();
        final SplitService splitService = state.getRuntimeSplitService();
        final AtomicReference<Runnable> checkpointAction =
                new AtomicReference<>(() -> {
                });
        final MaintenanceService maintenance = newMaintenance(splitService,
                state, () -> checkpointAction.get().run());
        final StorageService<K, V> storageService =
                state.getStorageService();
        initializeWal(state, storageService, maintenance);
        checkpointAction.set(storageService::checkpointWal);
        final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier =
                new SegmentRuntimeLimitApplier<>(state.getSegmentRegistry(),
                        state.getSegmentRegistry().runtime(),
                        state.getChunkStoreCache());
        final IndexRuntimeMonitoring runtimeMonitoring =
                newRuntimeMonitoring(state, splitService);
        final RuntimeTuning runtimeTuning = newRuntimeTuning(request, state,
                topologyRuntime, runtimeLimitApplier);
        state.setRuntimeServices(new SegmentIndexRuntimeServices<>(
                newOperationAccess(state, segmentLeaseService, storageService),
                maintenance, runtimeMonitoring, runtimeTuning));
    }

    private MaintenanceService newMaintenance(
            final SplitService splitService,
            final SegmentIndexBootstrapState<K, V> state,
            final Runnable checkpointAction) {
        final EffectiveIndexMaintenanceConfiguration maintenance =
                state.getConfiguration().maintenance();
        return MaintenanceService.<K, V>builder()
                .keyToSegmentMap(state.getKeyToSegmentMap())
                .stableSegmentGateway(StableSegmentOperationAccess.create(
                        state.getSegmentRegistry()))
                .splitService(splitService)
                .busyBackoffMillis(maintenance.busyBackoffMillis())
                .busyTimeoutMillis(maintenance.busyTimeoutMillis())
                .statsRecorder(sessionResources.maintenanceStatsRecorder())
                .maintenanceExecutor(
                        state.getExecutorRegistry()
                                .getIndexMaintenanceExecutor())
                .checkpointAction(checkpointAction)
                .build();
    }

    private void initializeWal(
            final SegmentIndexBootstrapState<K, V> state,
            final StorageService<K, V> storageService,
            final MaintenanceService maintenance) {
        storageService.initializeWal(state.getConfiguration(),
                state.hasRuntimeWalRuntime() ? state.getRuntimeWalRuntime() : null,
                () -> { },
                maintenance::flushAndWait, sessionResources::currentState,
                sessionResources::markRuntimeFailure,
                state.lastAppliedWalLsn());
    }

    private SegmentIndexOperationAccess<K, V> newOperationAccess(
            final SegmentIndexBootstrapState<K, V> state,
            final SegmentLeaseService<K, V> segmentLeaseService,
            final StorageService<K, V> storageService) {
        return SegmentIndexOperationAccess.create(
                state.getValueTypeDescriptor(),
                sessionResources.operationStatsRecorder(),
                segmentLeaseService,
                storageService);
    }

    private IndexRuntimeMonitoring newRuntimeMonitoring(
            final SegmentIndexBootstrapState<K, V> state,
            final SplitService splitService) {
        return IndexRuntimeMonitoring.<K, V>builder()
                .withConf(state.getConfiguration())
                .withKeyToSegmentMap(state.getKeyToSegmentMap())
                .withSegmentRegistry(state.getSegmentRegistry())
                .withSplitStatsView(splitService.splitStatsView())
                .withExecutorRegistry(state.getExecutorRegistry())
                .withRuntimeTuningState(state.getRuntimeTuningState())
                .withChunkStoreCache(state.getChunkStoreCache())
                .withWalMonitoringView(walMonitoringView(state))
                .withIndexOperationStatsRecorder(
                        sessionResources.operationStatsRecorder())
                .withMaintenanceStatsRecorder(
                        sessionResources.maintenanceStatsRecorder())
                .withCompactRequestHighWaterMark(
                        state.compactRequestHighWaterMark())
                .withFlushRequestHighWaterMark(
                        state.flushRequestHighWaterMark())
                .withLastAppliedWalLsn(state.lastAppliedWalLsn())
                .withStateView(sessionResources)
                .build();
    }

    private WalMonitoringView walMonitoringView(
            final SegmentIndexBootstrapState<K, V> state) {
        if (!state.getConfiguration().wal().isEnabled()) {
            return WalMonitoringView.empty();
        }
        return state.getRuntimeWalRuntime();
    }

    private RuntimeTuning newRuntimeTuning(
            final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state,
            final SegmentTopologyRuntimeAccess<K, V> topologyRuntime,
            final SegmentRuntimeLimitApplier<K, V> runtimeLimitApplier) {
        final Directory directory = request.getDirectory();
        final EffectiveIndexConfiguration<K, V> configuration =
                state.getConfiguration();
        return new RuntimeTuningServiceImpl(state.getRuntimeTuningState(),
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
