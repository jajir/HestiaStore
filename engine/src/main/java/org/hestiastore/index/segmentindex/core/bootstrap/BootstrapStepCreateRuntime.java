package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexRuntime;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;

/**
 * Creates runtime resources and owns failed-startup runtime cleanup.
 */
final class BootstrapStepCreateRuntime<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private final SegmentIndexSessionResources<K, V> sessionResources;
    private SegmentIndexBootstrapState<K, V> state;

    BootstrapStepCreateRuntime(
            final SegmentIndexSessionResources<K, V> sessionResources) {
        this.sessionResources = Vldtn.requireNonNull(sessionResources,
                "sessionResources");
    }

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        this.state = state;
        final SegmentIndexRuntime<K, V> runtime = new SegmentIndexRuntime<>(
                state.getKeyTypeDescriptor(),
                state.getCoreStorage(),
                state.getRuntimeTopologyRuntime(),
                state.getRuntimeServices());
        sessionResources.setRuntime(runtime, state.getExecutorRegistry());
        state.markIndexRuntimeCreated();
    }

    @Override
    void closeResource() {
        if (state != null && state.indexRuntimeWasCreated()) {
            sessionResources.closeRuntimeAfterFailedInitialization();
        }
    }
}
