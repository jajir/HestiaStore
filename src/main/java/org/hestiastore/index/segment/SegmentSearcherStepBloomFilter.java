package org.hestiastore.index.segment;

import org.hestiastore.index.Filter;
import org.hestiastore.index.Vldtn;

/**
 * Second step: consult bloom filter and short-circuit if key is definitely
 * absent.
 */
final class SegmentSearcherStepBloomFilter<K, V>
        implements Filter<SegmentSearcherContext<K, V>, SegmentSearcherResult<V>> {

    SegmentSearcherStepBloomFilter() {
    }

    @Override
    public boolean filter(final SegmentSearcherContext<K, V> ctx,
            final SegmentSearcherResult<V> result) {
        Vldtn.requireNonNull(ctx, "ctx");
        Vldtn.requireNonNull(result, "result");
        if (ctx.isNotStoredInBloomFilter()) {
            result.setValue(null);
            return false;
        }
        return true;
    }
}
