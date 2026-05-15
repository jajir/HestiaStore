package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.segmentindex.logging.IndexMdcCallWrapper;

/**
 * Creates the index MDC call wrapper when context logging is enabled.
 */
final class BootstrapStepCreateMdcCallWrapper<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        if (state.getConfiguration().logging().contextEnabled()) {
            state.setIndexMdcCallWrapper(new IndexMdcCallWrapper(
                    state.getConfiguration().identity().name()));
        }
    }
}
