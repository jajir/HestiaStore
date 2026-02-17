package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.Filter;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.Vldtn;

final class SegmentSplitStepFillLowerUntilTarget<K, V>
        implements Filter<SegmentSplitContext<K, V>, SegmentSplitState<K, V>> {

    /** {@inheritDoc} */
    @Override
    public boolean filter(final SegmentSplitContext<K, V> ctx,
            final SegmentSplitState<K, V> state) {
        Vldtn.requireNonNull(ctx, "ctx");
        Vldtn.requireNonNull(state, "state");
        Vldtn.requireNonNull(ctx.getPlan(), "plan");
        Vldtn.requireNonNull(ctx.getWriterTxFactory(), "writerTxFactory");
        Vldtn.requireNonNull(state.getLowerSegmentId(), "lowerSegmentId");
        Vldtn.requireNonNull(state.getIterator(), "iterator");
        final WriteTransaction<K, V> lowerSegmentWriteTx = ctx
                .getWriterTxFactory().openWriterTx(state.getLowerSegmentId());
        try (EntryWriter<K, V> writer = lowerSegmentWriteTx.open()) {
            while (ctx.getPlan().getLowerCount() < ctx.getPlan().getHalf()
                    && state.getIterator().hasNext()) {
                final Entry<K, V> entry = state.getIterator().next();
                ctx.getPlan().recordLower(entry);
                writer.write(entry);
            }
        }
        lowerSegmentWriteTx.commit();
        return true;
    }
}
