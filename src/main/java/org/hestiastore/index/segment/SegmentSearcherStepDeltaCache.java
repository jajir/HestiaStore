package org.hestiastore.index.segment;

import org.hestiastore.index.Filter;
import org.hestiastore.index.Vldtn;

/**
 * First step: read from delta cache and short-circuit if found or tombstone.
 */
final class SegmentSearcherStepDeltaCache<K, V> implements
        Filter<SegmentSearcherContext<K, V>, SegmentSearcherResult<V>> {

    private final org.hestiastore.index.datatype.TypeDescriptor<V> valueTypeDescriptor;

    SegmentSearcherStepDeltaCache(
            final org.hestiastore.index.datatype.TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
    }

    @Override
    public boolean filter(final SegmentSearcherContext<K, V> ctx,
            final SegmentSearcherResult<V> result) {
        Vldtn.requireNonNull(ctx, "ctx");
        Vldtn.requireNonNull(result, "result");
        final V fromCache = ctx.getDeltaCache().get(ctx.getKey());
        if (valueTypeDescriptor.isTombstone(fromCache)) {
            result.setValue(null);
            return false;
        }
        if (fromCache != null) {
            result.setValue(fromCache);
            return false;
        }
        return true;
    }
}
