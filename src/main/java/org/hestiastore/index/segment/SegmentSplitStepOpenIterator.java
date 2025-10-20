package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

final class SegmentSplitStepOpenIterator<K, V>
        implements SegmentSplitStep<K, V> {

    @Override
    public SegmentSplitterResult<K, V> perform(
            final SegmentSplitContext<K, V> ctx,
            final SegmentSplitState<K, V> state) {
        Vldtn.requireNonNull(ctx, "ctx");
        Vldtn.requireNonNull(state, "state");
        Vldtn.requireNonNull(ctx.getSegment(), "segment");
        state.setIterator(ctx.getSegment().openIterator());
        return null;
    }
}
