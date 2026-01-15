package org.hestiastore.index.segment;

import org.hestiastore.index.Filter;
import org.hestiastore.index.Vldtn;

/**
 * Final step: locate position via scarce index and read from index file.
 */
final class SegmentSearcherStepIndexFile<K, V>
        implements Filter<SegmentSearcherContext<K, V>, SegmentSearcherResult<V>> {

    /**
     * Creates the index-file lookup step.
     */
    SegmentSearcherStepIndexFile() {
    }

    /**
     * Looks up the key using scarce index position and index file scan.
     *
     * @param ctx search context
     * @param result result holder
     * @return always false to stop the pipeline after this step
     */
    @Override
    public boolean filter(final SegmentSearcherContext<K, V> ctx,
            final SegmentSearcherResult<V> result) {
        Vldtn.requireNonNull(ctx, "ctx");
        Vldtn.requireNonNull(result, "result");
        final Integer position = ctx.getPositionFromScarceIndex();
        if (position == null) {
            result.setValue(null);
            return false;
        }
        final V value = ctx.searchInIndex(position);
        if (value == null) {
            ctx.incrementFalsePositive();
            result.setValue(null);
            return false;
        }
        result.setValue(value);
        return false;
    }
}
