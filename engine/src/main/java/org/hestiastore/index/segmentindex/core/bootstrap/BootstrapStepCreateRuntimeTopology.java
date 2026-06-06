package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.segmentlease.SegmentLeaseService;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;
import org.hestiastore.index.segmentindex.core.session.SegmentTopologyRuntimeAccess;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationAccess;
import org.hestiastore.index.segmentindex.core.storage.SegmentIndexCoreStorage;
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
        final SegmentIndexCoreStorage<K, V> coreStorage =
                state.getCoreStorage();
        final SegmentTopology<K> segmentTopology =
                newSegmentTopology(coreStorage);
        final SegmentLeaseService<K, V> segmentLeaseService =
                newSegmentLeaseService(coreStorage, segmentTopology);
        splitService = newSplitService(request, state, coreStorage,
                segmentLeaseService);
        final SegmentTopologyRuntimeAccess<K, V> topologyRuntime =
                newTopologyRuntime(coreStorage, splitService);
        state.setRuntimeSegmentLeaseService(segmentLeaseService);
        state.setRuntimeSplitService(splitService);
        state.setRuntimeTopologyRuntime(topologyRuntime);
    }

    @Override
    void closeResource() {
        if (state == null || state.indexRuntimeWasCreated()
                || splitService == null) {
            return;
        }
        splitService.close();
    }

    private SegmentTopology<K> newSegmentTopology(
            final SegmentIndexCoreStorage<K, V> coreStorage) {
        return SegmentTopology.<K>builder()
                .snapshot(coreStorage.keyToSegmentMap().snapshot())
                .retryPolicy(coreStorage.retryPolicy())
                .build();
    }

    private SegmentLeaseService<K, V> newSegmentLeaseService(
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final SegmentTopology<K> segmentTopology) {
        return SegmentLeaseService.<K, V>builder()
                .keyToSegmentMap(coreStorage.keyToSegmentMap())
                .segmentRegistry(coreStorage.segmentRegistry())
                .segmentTopology(segmentTopology)
                .retryPolicy(coreStorage.retryPolicy())
                .build();
    }

    private SplitService newSplitService(
            final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state,
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final SegmentLeaseService<K, V> segmentLeaseService) {
        return SplitService.<K, V>builder()
                .conf(state.getConfiguration())
                .runtimeTuningState(coreStorage.runtimeTuningState())
                .keyToSegmentMap(coreStorage.keyToSegmentMap())
                .segmentLeaseService(segmentLeaseService)
                .segmentRegistry(coreStorage.segmentRegistry())
                .directoryFacade(request.getDirectory())
                .splitExecutor(state.getExecutorRegistry()
                        .getSplitMaintenanceExecutor())
                .workerExecutor(state.getExecutorRegistry()
                        .getIndexMaintenanceExecutor())
                .splitPolicyScheduler(state.getExecutorRegistry()
                        .getSplitPolicyScheduler())
                .stateSupplier(sessionResources::currentState)
                .failureHandler(sessionResources::markRuntimeFailure)
                .statsRecorder(sessionResources.splitStatsRecorder())
                .build();
    }

    private SegmentTopologyRuntimeAccess<K, V> newTopologyRuntime(
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final SplitService splitService) {
        final StableSegmentOperationAccess<K, V> stableSegmentGateway =
                StableSegmentOperationAccess.create(
                        coreStorage.segmentRegistry());
        final SegmentStreamingService<K, V> streamingService =
                newStreamingService(coreStorage, stableSegmentGateway);
        final DirectSegmentAccess<K, V> directSegmentAccess =
                DirectSegmentAccess.create(coreStorage.keyToSegmentMap(),
                        coreStorage.segmentRegistry(),
                        coreStorage.retryPolicy());
        return SegmentTopologyRuntimeAccess.create(splitService,
                streamingService, directSegmentAccess);
    }

    private SegmentStreamingService<K, V> newStreamingService(
            final SegmentIndexCoreStorage<K, V> coreStorage,
            final StableSegmentOperationAccess<K, V> stableSegmentGateway) {
        return SegmentStreamingService.<K, V>builder()
                .keyToSegmentMap(coreStorage.keyToSegmentMap())
                .segmentRegistry(coreStorage.segmentRegistry())
                .stableSegmentGateway(stableSegmentGateway)
                .retryPolicy(coreStorage.retryPolicy())
                .build();
    }
}
