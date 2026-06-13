package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexSessionResources;

/**
 * Transfers failed-bootstrap runtime cleanup ownership away from earlier
 * opening steps.
 */
final class BootstrapStepTransferRuntimeCloseOwnership<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private final SegmentIndexSessionResources<K, V> sessionResources;
    private SegmentIndexBootstrapState<K, V> state;
    private BootstrapRuntimeCloseResources<K, V> closeResources;

    BootstrapStepTransferRuntimeCloseOwnership(
            final SegmentIndexSessionResources<K, V> sessionResources) {
        this.sessionResources = Vldtn.requireNonNull(sessionResources,
                "sessionResources");
    }

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        this.state = state;
        closeResources = new BootstrapRuntimeCloseResources<>(
                state.getRuntimeTopologyRuntime(),
                state.getCoreStorageRuntime(),
                state.getStorageService());
        sessionResources.setExecutorRegistry(state.getExecutorRegistry());
        state.markRuntimeCloseOwnershipTransferred();
    }

    @Override
    void closeResource() {
        if (state != null && state.runtimeCloseOwnershipTransferred()
                && closeResources != null) {
            closeResources.closeAfterFailedInitialization();
        }
    }
}
