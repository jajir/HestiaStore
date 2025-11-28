package org.hestiastore.index.segment;

import org.hestiastore.index.Filter;
import org.hestiastore.index.Vldtn;

final class SegmentSplitStepReplaceIfNoRemaining<K, V>
        implements Filter<SegmentSplitContext<K, V>, SegmentSplitState<K, V>> {

    private final SegmentReplacer<K, V> segmentReplacer;

    SegmentSplitStepReplaceIfNoRemaining(
            final SegmentReplacer<K, V> segmentReplacer) {
        this.segmentReplacer = segmentReplacer;
    }

    @Override
    public boolean filter(final SegmentSplitContext<K, V> ctx,
            final SegmentSplitState<K, V> state) {
        Vldtn.requireNonNull(ctx, "ctx");
        Vldtn.requireNonNull(state, "state");
        Vldtn.requireNonNull(state.getIterator(), "iterator");
        Vldtn.requireNonNull(state.getLowerSegment(), "lowerSegment");
        if (state.getIterator().hasNext()) {
            return true; // continue to next step (split upper)
        }
        segmentReplacer.replaceWithLower(state.getLowerSegment());
        state.setResult(new SegmentSplitterResult<>(state.getLowerSegment(),
                ctx.getPlan().getMinKey(), ctx.getPlan().getMaxKey(),
                SegmentSplitterResult.SegmentSplittingStatus.COMPACTED));
        return false;
    }
}
