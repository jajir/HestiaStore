package org.hestiastore.index.segmentindex;

import java.util.List;

import org.hestiastore.index.AbstractChainOfFilters;
import org.hestiastore.index.Filter;

/**
 * Executes the ordered set of split steps for a single segment split.
 * <p>
 * Delegates step execution to the shared {@link AbstractChainOfFilters}
 * machinery and ensures the iterator opened by earlier steps is always closed.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentSplitPipeline<K, V>
        extends AbstractChainOfFilters<SegmentSplitContext<K, V>, SegmentSplitState<K, V>> {

    SegmentSplitPipeline(final List<Filter<SegmentSplitContext<K, V>, SegmentSplitState<K, V>>> steps) {
        super(List.copyOf(steps));
    }

    SegmentSplitterResult<K, V> run(final SegmentSplitContext<K, V> ctx) {
        final SegmentSplitState<K, V> state = runKeepingIterator(ctx);
        try {
            return state.getResult();
        } finally {
            if (state.getIterator() != null) {
                state.getIterator().close();
            }
        }
    }

    SegmentSplitState<K, V> runKeepingIterator(
            final SegmentSplitContext<K, V> ctx) {
        final SegmentSplitState<K, V> state = new SegmentSplitState<>();
        try {
            filter(ctx, state);
            if (state.getResult() != null) {
                return state;
            }
            throw new IllegalStateException(
                    "Split pipeline produced no result");
        } catch (final RuntimeException e) {
            if (state.getIterator() != null) {
                state.getIterator().close();
            }
            throw e;
        }
    }
}
