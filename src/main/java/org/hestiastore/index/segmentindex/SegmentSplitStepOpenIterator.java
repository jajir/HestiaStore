package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Filter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentIteratorIsolation;

final class SegmentSplitStepOpenIterator<K, V>
        implements Filter<SegmentSplitContext<K, V>, SegmentSplitState<K, V>> {

    @Override
    public boolean filter(final SegmentSplitContext<K, V> ctx,
            final SegmentSplitState<K, V> state) {
        Vldtn.requireNonNull(ctx, "ctx");
        Vldtn.requireNonNull(state, "state");
        Vldtn.requireNonNull(ctx.getSegment(), "segment");
        state.setIterator(ctx.getSegment()
                .openIterator(SegmentIteratorIsolation.FULL_ISOLATION));
        return true;
    }
}
