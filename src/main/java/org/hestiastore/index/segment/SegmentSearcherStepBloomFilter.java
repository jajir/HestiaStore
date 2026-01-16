package org.hestiastore.index.segment;

import org.hestiastore.index.Filter;
import org.hestiastore.index.Vldtn;

/**
 * First step: consult bloom filter and short-circuit if key is definitely
 * absent.
 */
final class SegmentSearcherStepBloomFilter<K, V>
        implements Filter<SegmentSearcherContext<K, V>, SegmentSearcherResult<V>> {

    /**
     * Creates the Bloom filter lookup step.
     */
    SegmentSearcherStepBloomFilter() {
    }

    /**
     * Checks the Bloom filter and short-circuits when the key is absent.
     *
     * @param ctx search context
     * @param result result holder
     * @return true to continue pipeline, false to stop
     */
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
