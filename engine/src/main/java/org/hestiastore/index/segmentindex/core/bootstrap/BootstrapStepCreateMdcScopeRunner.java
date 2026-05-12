package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.segmentindex.core.IndexMdcScopeRunner;

/**
 * Creates the index MDC scope runner when context logging is enabled.
 */
final class BootstrapStepCreateMdcScopeRunner<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        if (state.getConfiguration().logging().contextEnabled()) {
            state.setIndexMdcScopeRunner(new IndexMdcScopeRunner(
                    state.getConfiguration().identity().name()));
        }
    }
}
