package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoringBuilder;
import org.hestiastore.index.segmentindex.wal.WalMonitoringView;

/**
 * Creates runtime monitoring views after storage, topology, and WAL are ready.
 */
final class BootstrapStepCreateRuntimeMonitoring<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private final SegmentIndexSessionResources<K, V> sessionResources;

    BootstrapStepCreateRuntimeMonitoring(
            final SegmentIndexSessionResources<K, V> sessionResources) {
        this.sessionResources = Vldtn.requireNonNull(sessionResources,
                "sessionResources");
    }

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        final SplitService splitService = state.getRuntimeSplitService();
        state.setRuntimeMonitoring(newRuntimeMonitoring(state, splitService));
    }

    private IndexRuntimeMonitoring newRuntimeMonitoring(
            final SegmentIndexBootstrapState<K, V> state,
            final SplitService splitService) {
        return IndexRuntimeMonitoringBuilder.<K, V>builder()
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
}
