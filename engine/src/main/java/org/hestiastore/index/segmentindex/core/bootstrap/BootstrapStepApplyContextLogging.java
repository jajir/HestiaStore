package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.segmentindex.logging.SegmentIndexMdcLoggingAdapter;
import org.hestiastore.index.segmentindex.logging.IndexMdcCallWrapper;

/**
 * Wraps the current index handle with MDC context logging when enabled.
 */
final class BootstrapStepApplyContextLogging<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        final var configuration = state.getConfiguration();
        if (!configuration.logging().contextEnabled()) {
            return;
        }
        state.setIndexHandle(new SegmentIndexMdcLoggingAdapter<>(
                state.getIndexHandle(), new IndexMdcCallWrapper(
                        configuration.identity().name())));
    }
}
