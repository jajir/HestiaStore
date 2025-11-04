package org.hestiastore.index.segment;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.WriteTransaction;
import org.hestiastore.index.Vldtn;

final class SegmentSplitStepFillLowerUntilTarget<K, V>
        implements SegmentSplitStep<K, V> {

    @Override
    public SegmentSplitterResult<K, V> perform(
            final SegmentSplitContext<K, V> ctx,
            final SegmentSplitState<K, V> state) {
        Vldtn.requireNonNull(ctx, "ctx");
        Vldtn.requireNonNull(state, "state");
        Vldtn.requireNonNull(ctx.getPlan(), "plan");
        Vldtn.requireNonNull(state.getLowerSegment(), "lowerSegment");
        Vldtn.requireNonNull(state.getIterator(), "iterator");
        final WriteTransaction<K, V> lowerSegmentWriteTx = state
                .getLowerSegment().openFullWriteTx();
        try (EntryWriter<K, V> writer = lowerSegmentWriteTx.open()) {
            while (ctx.getPlan().getLowerCount() < ctx.getPlan().getHalf()
                    && state.getIterator().hasNext()) {
                final Entry<K, V> entry = state.getIterator().next();
                ctx.getPlan().recordLower(entry);
                writer.write(entry);
            }
        }
        lowerSegmentWriteTx.commit();
        return null;
    }
}
