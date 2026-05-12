package org.hestiastore.index.segmentindex.core.bootstrap;

/**
 * Completes startup recovery and marks the index running.
 */
final class BootstrapStepCompleteStartup<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        state.getInternalIndex().completeStartup();
    }
}
