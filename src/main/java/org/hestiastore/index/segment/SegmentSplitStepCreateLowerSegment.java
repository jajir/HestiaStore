package org.hestiastore.index.segment;

import org.hestiastore.index.Filter;
import org.hestiastore.index.Vldtn;

final class SegmentSplitStepCreateLowerSegment<K, V>
        implements Filter<SegmentSplitContext<K, V>, SegmentSplitState<K, V>> {

    @Override
    public boolean filter(final SegmentSplitContext<K, V> ctx,
            final SegmentSplitState<K, V> state) {
        Vldtn.requireNonNull(ctx, "ctx");
        Vldtn.requireNonNull(state, "state");
        Vldtn.requireNonNull(ctx.getSegment(), "segment");
        Vldtn.requireNonNull(ctx.getLowerSegmentId(), "lowerSegmentId");
        state.setLowerSegment(ctx.getSegment()
                .createSegmentWithSameConfig(ctx.getLowerSegmentId()));
        return true;
    }
}
