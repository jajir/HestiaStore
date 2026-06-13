package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionAssemblyInput;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionFactory;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;

/**
 * Creates facades, session owner, maintenance API, and session index handle.
 */
final class BootstrapStepCreateIndex<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private final SegmentIndexSessionResources<K, V> sessionResources;

    BootstrapStepCreateIndex(
            final SegmentIndexSessionResources<K, V> sessionResources) {
        this.sessionResources = Vldtn.requireNonNull(sessionResources,
                "sessionResources");
    }

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        state.setIndexHandle(SegmentIndexSessionFactory.createIndex(
                sessionResources,
                state.getConfiguration(), state.getKeyTypeDescriptor(),
                new SegmentIndexSessionAssemblyInput<>(
                        state.getRuntimeOperationAccess(),
                        state.getRuntimeTopologyRuntime(),
                        state.getRuntimeMaintenanceService(),
                        state.getRuntimeTuning(),
                        state.getRuntimeMonitoring(),
                        state.getCoreStorageRuntime(),
                        state.getStorageService())));
    }
}
