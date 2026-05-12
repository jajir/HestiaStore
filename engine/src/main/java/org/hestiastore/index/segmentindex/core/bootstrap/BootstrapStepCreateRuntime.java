package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;

/**
 * Creates runtime resources and owns failed-startup runtime cleanup.
 */
final class BootstrapStepCreateRuntime<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private final SegmentIndexSessionResources<K, V> sessionResources;

    BootstrapStepCreateRuntime(
            final SegmentIndexSessionResources<K, V> sessionResources) {
        this.sessionResources = Vldtn.requireNonNull(sessionResources,
                "sessionResources");
    }

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        sessionResources.createRuntime(request.getDirectory(),
                state.getKeyTypeDescriptor(), state.getValueTypeDescriptor(),
                state.getConfiguration(), state.getExecutorRegistry());
    }

    @Override
    void closeResource() {
        sessionResources.closeRuntimeAfterFailedInitialization();
    }
}
