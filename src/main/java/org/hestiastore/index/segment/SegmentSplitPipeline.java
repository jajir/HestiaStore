package org.hestiastore.index.segment;

import java.util.List;

final class SegmentSplitPipeline<K, V> {

    private final List<SegmentSplitStep<K, V>> steps;

    SegmentSplitPipeline(final List<SegmentSplitStep<K, V>> steps) {
        this.steps = List.copyOf(steps);
    }

    SegmentSplitterResult<K, V> run(final SegmentSplitContext<K, V> ctx) {
        final SegmentSplitState<K, V> state = new SegmentSplitState<>();
        try {
            for (final SegmentSplitStep<K, V> step : steps) {
                final SegmentSplitterResult<K, V> res = step.perform(ctx, state);
                if (res != null) {
                    return res;
                }
            }
            throw new IllegalStateException(
                    "Split pipeline produced no result");
        } finally {
            if (state.getIterator() != null) {
                state.getIterator().close();
            }
        }
    }
}
