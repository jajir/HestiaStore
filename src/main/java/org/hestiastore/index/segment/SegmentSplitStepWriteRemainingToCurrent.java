package org.hestiastore.index.segment;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.Filter;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.Vldtn;

final class SegmentSplitStepWriteRemainingToCurrent<K, V>
        implements Filter<SegmentSplitContext<K, V>, SegmentSplitState<K, V>> {

    @Override
    public boolean filter(final SegmentSplitContext<K, V> ctx,
            final SegmentSplitState<K, V> state) {
        Vldtn.requireNonNull(ctx, "ctx");
        Vldtn.requireNonNull(state, "state");
        Vldtn.requireNonNull(ctx.getSegment(), "segment");
        Vldtn.requireNonNull(ctx.getPlan(), "plan");
        Vldtn.requireNonNull(state.getIterator(), "iterator");
        Vldtn.requireNonNull(state.getLowerSegment(), "lowerSegment");
        final WriteTransaction<K, V> segmentWriteTx = ctx.getSegment()
                .openFullWriteTx();
        try (EntryWriter<K, V> writer = segmentWriteTx.open()) {
            while (state.getIterator().hasNext()) {
                final Entry<K, V> entry = state.getIterator().next();
                writer.write(entry);
                ctx.getPlan().recordUpper();
            }
        }
        segmentWriteTx.commit();
        state.setResult(new SegmentSplitterResult<>(state.getLowerSegment(),
                ctx.getPlan().getMinKey(), ctx.getPlan().getMaxKey(),
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT));
        return false;
    }
}
