package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

final class SegmentSplitStepValidateFeasibility<K, V>
        implements SegmentSplitStep<K, V> {

    @Override
    public SegmentSplitterResult<K, V> perform(
            final SegmentSplitContext<K, V> ctx,
            final SegmentSplitState<K, V> state) {
        Vldtn.requireNonNull(ctx, "ctx");
        Vldtn.requireNonNull(state, "state");
        Vldtn.requireNonNull(ctx.getPlan(), "plan");
        if (!ctx.getPlan().isSplitFeasible()) {
            throw new IllegalStateException(
                    "Splitting failed. Number of keys is too low.");
        }
        return null;
    }
}
