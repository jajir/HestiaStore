package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.segmentindex.core.session.SegmentIndexResourceClosingAdapter;

/**
 * Wraps the current index handle with normal-close resource cleanup.
 */
final class BootstrapStepWrapResourceClosing<K, V>
        extends SegmentIndexBootstrapStep<K, V> {
    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        final SegmentIndexResourceClosingAdapter<K, V> returnedIndex = new SegmentIndexResourceClosingAdapter<>(
                state.getIndexHandle());
        state.setIndex(returnedIndex);
    }
}
