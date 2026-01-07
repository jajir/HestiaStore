package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Filter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.hestiastore.index.segment.SegmentIteratorIsolation;

final class SegmentSplitStepOpenIterator<K, V>
        implements Filter<SegmentSplitContext<K, V>, SegmentSplitState<K, V>> {

    @Override
    public boolean filter(final SegmentSplitContext<K, V> ctx,
            final SegmentSplitState<K, V> state) {
        Vldtn.requireNonNull(ctx, "ctx");
        Vldtn.requireNonNull(state, "state");
        Vldtn.requireNonNull(ctx.getSegment(), "segment");
        final SegmentResult<org.hestiastore.index.EntryIterator<K, V>> result = ctx
                .getSegment()
                .openIterator(SegmentIteratorIsolation.FULL_ISOLATION);
        if (result.getStatus() != SegmentResultStatus.OK) {
            throw new org.hestiastore.index.IndexException(String.format(
                    "Segment '%s' failed to open iterator: %s",
                    ctx.getSegment().getId(), result.getStatus()));
        }
        state.setIterator(result.getValue());
        return true;
    }
}
