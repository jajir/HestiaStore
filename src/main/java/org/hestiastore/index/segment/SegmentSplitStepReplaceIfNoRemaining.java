package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

final class SegmentSplitStepReplaceIfNoRemaining<K, V>
        implements SegmentSplitStep<K, V> {

    private final SegmentReplacer<K, V> segmentReplacer;

    SegmentSplitStepReplaceIfNoRemaining(
            final SegmentReplacer<K, V> segmentReplacer) {
        this.segmentReplacer = segmentReplacer;
    }

    @Override
    public SegmentSplitterResult<K, V> perform(
            final SegmentSplitContext<K, V> ctx,
            final SegmentSplitState<K, V> state) {
        Vldtn.requireNonNull(ctx, "ctx");
        Vldtn.requireNonNull(state, "state");
        Vldtn.requireNonNull(state.getIterator(), "iterator");
        Vldtn.requireNonNull(state.getLowerSegment(), "lowerSegment");
        if (state.getIterator().hasNext()) {
            return null; // continue to next step (split upper)
        }
        segmentReplacer.replaceWithLower(state.getLowerSegment());
        return new SegmentSplitterResult<>(state.getLowerSegment(),
                ctx.getPlan().getMinKey(), ctx.getPlan().getMaxKey(),
                SegmentSplitterResult.SegmentSplittingStatus.COMPACTED);
    }
}
