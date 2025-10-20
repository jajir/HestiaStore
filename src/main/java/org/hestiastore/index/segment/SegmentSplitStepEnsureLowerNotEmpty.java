package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

final class SegmentSplitStepEnsureLowerNotEmpty<K, V>
        implements SegmentSplitStep<K, V> {

    @Override
    public SegmentSplitterResult<K, V> perform(
            final SegmentSplitContext<K, V> ctx,
            final SegmentSplitState<K, V> state) {
        Vldtn.requireNonNull(ctx, "ctx");
        Vldtn.requireNonNull(state, "state");
        Vldtn.requireNonNull(ctx.getPlan(), "plan");
        if (ctx.getPlan().isLowerSegmentEmpty()) {
            throw new IllegalStateException(
                    "Splitting failed. Lower segment doesn't contains any data");
        }
        return null;
    }
}
