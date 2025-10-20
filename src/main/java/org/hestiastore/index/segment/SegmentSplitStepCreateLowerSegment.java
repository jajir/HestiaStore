package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

final class SegmentSplitStepCreateLowerSegment<K, V>
        implements SegmentSplitStep<K, V> {

    @Override
    public SegmentSplitterResult<K, V> perform(
            final SegmentSplitContext<K, V> ctx,
            final SegmentSplitState<K, V> state) {
        Vldtn.requireNonNull(ctx, "ctx");
        Vldtn.requireNonNull(state, "state");
        Vldtn.requireNonNull(ctx.getSegment(), "segment");
        Vldtn.requireNonNull(ctx.getLowerSegmentId(), "lowerSegmentId");
        state.setLowerSegment(ctx.getSegment()
                .createSegmentWithSameConfig(ctx.getLowerSegmentId()));
        return null;
    }
}
