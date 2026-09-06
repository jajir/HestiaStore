package org.hestiastore.index.senku;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;

/**
 * Immutable approximate distribution of natural-order long keys. Each key
 * represents a positive number of records; weights sum to the exact record
 * count. Representatives are not exact global ordinal samples, and no formal
 * quantile-error bound is promised. They must never be used for membership.
 */
public final class SenkuLongKeySummary {

    /** Maximum number of representatives in an aggregate ready summary. */
    public static final int MAX_SAMPLES = 4096;

    private final long recordCount;
    private final long[] keys;
    private final long[] weights;

    private SenkuLongKeySummary(final long recordCount, final long[] keys,
            final long[] weights) {
        this.recordCount = recordCount;
        this.keys = keys;
        this.weights = weights;
    }

    /**
     * Creates a validated distribution, defensively copying both arrays.
     *
     * @param recordCount exact number of represented records
     * @param keys        strictly increasing natural-order representative keys
     * @param weights     positive weights summing to recordCount
     * @return immutable weighted summary
     */
    public static SenkuLongKeySummary of(final long recordCount,
            final long[] keys, final long[] weights) {
        Vldtn.requireGreaterThanOrEqualToZero(recordCount, "recordCount");
        Vldtn.requireNonNull(keys, "keys");
        Vldtn.requireNonNull(weights, "weights");
        Vldtn.requireTrue(
                keys.length == weights.length && keys.length <= MAX_SAMPLES,
                "Summary arrays must have equal bounded lengths");
        final long[] keyCopy = keys.clone();
        final long[] weightCopy = weights.clone();
        long sum = 0L;
        try {
            for (int index = 0; index < keyCopy.length; index++) {
                Vldtn.requireTrue(
                        index == 0 || keyCopy[index] > keyCopy[index - 1],
                        "Summary keys must be strictly increasing");
                Vldtn.requireGreaterThanZero(weightCopy[index], "weight");
                sum = Math.addExact(sum, weightCopy[index]);
            }
        } catch (ArithmeticException overflow) {
            throw new IndexException("Summary weight overflow.", overflow);
        }
        Vldtn.requireTrue(sum == recordCount,
                "Summary weights must equal the exact record count");
        return new SenkuLongKeySummary(recordCount, keyCopy, weightCopy);
    }

    /**
     * Merges weighted distributions and groups adjacent representatives into at
     * most MAX_SAMPLES bins. Counts stay exact; representative keys remain
     * approximate. Input distributions are merged once in natural key order.
     *
     * @param summaries complete distributions, not overlapping merge inputs
     * @return bounded aggregate distribution
     */
    public static SenkuLongKeySummary merge(
            final List<SenkuLongKeySummary> summaries) {
        Vldtn.requireNonNull(summaries, "summaries");
        final PriorityQueue<SenkuLongSummaryCursor> pending = new PriorityQueue<>(
                Comparator.comparingLong(SenkuLongSummaryCursor::key));
        long count = 0L;
        try {
            for (final SenkuLongKeySummary summary : summaries) {
                Vldtn.requireNonNull(summary, "summary");
                count = Math.addExact(count, summary.recordCount);
                if (summary.keys.length != 0) {
                    pending.add(new SenkuLongSummaryCursor(summary.keys,
                            summary.weights));
                }
            }
        } catch (ArithmeticException overflow) {
            throw new IndexException("Summary record count overflow.",
                    overflow);
        }
        final long targetWeight = count / MAX_SAMPLES
                + (count % MAX_SAMPLES == 0 ? 0 : 1);
        final long[] keys = new long[MAX_SAMPLES];
        final long[] weights = new long[MAX_SAMPLES];
        int size = 0;
        while (!pending.isEmpty()) {
            final SenkuLongSummaryCursor cursor = pending.remove();
            if (size == 0 || (weights[size - 1] >= targetWeight
                    && keys[size - 1] != cursor.key() && size < MAX_SAMPLES)) {
                keys[size++] = cursor.key();
            }
            weights[size - 1] = Math.addExact(weights[size - 1],
                    cursor.weight());
            if (cursor.advance()) {
                pending.add(cursor);
            }
        }
        return of(count, Arrays.copyOf(keys, size),
                Arrays.copyOf(weights, size));
    }

    /** @return exact sum of the represented record weights */
    public long recordCount() {
        return recordCount;
    }

    /** @return defensive copy of approximate representative keys */
    public long[] keys() {
        return keys.clone();
    }

    /** @return defensive copy of positive representative weights */
    public long[] weights() {
        return weights.clone();
    }
}
