package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.segmentindex.core.session.IndexContextLoggingAdapter;

/**
 * Wraps the internal index with MDC context logging when enabled.
 */
final class BootstrapStepApplyContextLogging<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        if (!state.hasIndexMdcCallWrapper()) {
            state.setManagedIndex(state.getInternalIndex());
            return;
        }
        state.setManagedIndex(new IndexContextLoggingAdapter<>(
                state.getInternalIndex(), state.getIndexMdcCallWrapper()));
    }
}
