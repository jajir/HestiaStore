package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Filter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

final class SegmentSplitStepReplaceIfNoRemaining<K, V>
        implements Filter<SegmentSplitContext<K, V>, SegmentSplitState<K, V>> {

    @Override
    public boolean filter(final SegmentSplitContext<K, V> ctx,
            final SegmentSplitState<K, V> state) {
        Vldtn.requireNonNull(ctx, "ctx");
        Vldtn.requireNonNull(state, "state");
        Vldtn.requireNonNull(ctx.getPlan(), "plan");
        Vldtn.requireNonNull(state.getIterator(), "iterator");
        final SegmentId lowerSegmentId = Vldtn.requireNonNull(
                state.getLowerSegmentId(), "lowerSegmentId");
        if (state.getIterator().hasNext()) {
            return true; // continue to next step (split upper)
        }
        state.setResult(new SegmentSplitterResult<>(lowerSegmentId,
                ctx.getPlan().getMinKey(), ctx.getPlan().getMaxKey(),
                SegmentSplitterResult.SegmentSplittingStatus.COMPACTED));
        return false;
    }
}
