package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexMaintenanceConfiguration;
import org.hestiastore.index.segmentindex.core.segmentlease.SegmentLeaseService;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;
import org.hestiastore.index.segmentindex.core.session.SegmentTopologyRuntimeAccess;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationAccess;
import org.hestiastore.index.segmentindex.core.streaming.DirectSegmentAccess;
import org.hestiastore.index.segmentindex.core.streaming.SegmentStreamingService;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;

/**
 * Creates runtime topology, split, lease, and streaming collaborators.
 */
final class BootstrapStepCreateRuntimeTopology<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private final SegmentIndexSessionResources<K, V> sessionResources;
    private SegmentIndexBootstrapState<K, V> state;
    private SplitService splitService;

    BootstrapStepCreateRuntimeTopology(
            final SegmentIndexSessionResources<K, V> sessionResources) {
        this.sessionResources = Vldtn.requireNonNull(sessionResources,
                "sessionResources");
    }

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        this.state = state;
        final SegmentTopology<K> segmentTopology =
                newSegmentTopology(state);
        final SegmentLeaseService<K, V> segmentLeaseService =
                newSegmentLeaseService(state, segmentTopology);
        splitService = newSplitService(request, state, segmentLeaseService);
        final SegmentTopologyRuntimeAccess<K, V> topologyRuntime =
                newTopologyRuntime(state, splitService);
        state.setRuntimeSegmentLeaseService(segmentLeaseService);
        state.setRuntimeSplitService(splitService);
        state.setRuntimeTopologyRuntime(topologyRuntime);
    }

    @Override
    void closeResource() {
        if (state == null || state.runtimeCloseOwnershipTransferred()
                || splitService == null) {
            return;
        }
        splitService.close();
    }

    private SegmentTopology<K> newSegmentTopology(
            final SegmentIndexBootstrapState<K, V> state) {
        final EffectiveIndexMaintenanceConfiguration maintenance =
                state.getConfiguration().maintenance();
        return SegmentTopology.<K>builder()
                .snapshot(state.getKeyToSegmentMap().snapshot())
                .busyBackoffMillis(maintenance.busyBackoffMillis())
                .busyTimeoutMillis(maintenance.busyTimeoutMillis())
                .build();
    }

    private SegmentLeaseService<K, V> newSegmentLeaseService(
            final SegmentIndexBootstrapState<K, V> state,
            final SegmentTopology<K> segmentTopology) {
        final EffectiveIndexMaintenanceConfiguration maintenance =
                state.getConfiguration().maintenance();
        return SegmentLeaseService.<K, V>builder()
                .keyToSegmentMap(state.getKeyToSegmentMap())
                .segmentRegistry(state.getSegmentRegistry())
                .segmentTopology(segmentTopology)
                .busyBackoffMillis(maintenance.busyBackoffMillis())
                .busyTimeoutMillis(maintenance.busyTimeoutMillis())
                .build();
    }

    private SplitService newSplitService(
            final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state,
            final SegmentLeaseService<K, V> segmentLeaseService) {
        return SplitService.<K, V>builder()
                .conf(state.getConfiguration())
                .runtimeTuningState(state.getRuntimeTuningState())
                .keyToSegmentMap(state.getKeyToSegmentMap())
                .segmentLeaseService(segmentLeaseService)
                .segmentRegistry(state.getSegmentRegistry())
                .directoryFacade(request.getDirectory())
                .splitExecutor(state.getExecutorRegistry()
                        .getSplitMaintenanceExecutor())
                .workerExecutor(state.getExecutorRegistry()
                        .getIndexMaintenanceExecutor())
                .splitPolicyScheduler(state.getExecutorRegistry()
                        .getSplitPolicyScheduler())
                .stateView(sessionResources)
                .failureReporter(sessionResources::markRuntimeFailure)
                .statsRecorder(sessionResources.splitStatsRecorder())
                .build();
    }

    private SegmentTopologyRuntimeAccess<K, V> newTopologyRuntime(
            final SegmentIndexBootstrapState<K, V> state,
            final SplitService splitService) {
        final StableSegmentOperationAccess<K, V> stableSegmentGateway =
                StableSegmentOperationAccess.create(
                        state.getSegmentRegistry());
        final SegmentStreamingService<K, V> streamingService =
                newStreamingService(state, stableSegmentGateway);
        final DirectSegmentAccess<K, V> directSegmentAccess =
                DirectSegmentAccess.create(state.getKeyToSegmentMap(),
                        state.getSegmentRegistry(),
                        state.getConfiguration().maintenance()
                                .busyBackoffMillis(),
                        state.getConfiguration().maintenance()
                                .busyTimeoutMillis());
        return SegmentTopologyRuntimeAccess.create(splitService,
                streamingService, directSegmentAccess);
    }

    private SegmentStreamingService<K, V> newStreamingService(
            final SegmentIndexBootstrapState<K, V> state,
            final StableSegmentOperationAccess<K, V> stableSegmentGateway) {
        final EffectiveIndexMaintenanceConfiguration maintenance =
                state.getConfiguration().maintenance();
        return SegmentStreamingService.<K, V>builder()
                .keyToSegmentMap(state.getKeyToSegmentMap())
                .segmentRegistry(state.getSegmentRegistry())
                .stableSegmentGateway(stableSegmentGateway)
                .busyBackoffMillis(maintenance.busyBackoffMillis())
                .busyTimeoutMillis(maintenance.busyTimeoutMillis())
                .build();
    }
}
