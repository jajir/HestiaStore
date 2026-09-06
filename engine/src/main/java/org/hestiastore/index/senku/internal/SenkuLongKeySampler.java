package org.hestiastore.index.senku.internal;

import java.util.Arrays;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.senku.SenkuLongKeySummary;

/**
 * Samples one sorted, deduplicated output run using power-of-two thinning. Only
 * selected keys need decoding; each retained key represents the records up to
 * the next retained ordinal. Never merges summaries of input runs.
 */
final class SenkuLongKeySampler {

    static final int CAPACITY = 256;

    private final long[] keys = new long[CAPACITY];
    private long count;
    private long stride = 1L;
    private int size;
    private boolean selectedPending;

    /**
     * Counts the next emitted unique record and decides whether its logical key
     * is needed. A true result must be followed by addSelectedKey.
     *
     * @return whether the caller must decode and supply this key
     */
    boolean selectNext() {
        Vldtn.requireTrue(!selectedPending,
                "A selected summary key is missing");
        final long ordinal = count;
        try {
            count = Math.incrementExact(count);
            if ((ordinal & (stride - 1)) == 0L && size == CAPACITY) {
                for (int index = 0; index < CAPACITY / 2; index++) {
                    keys[index] = keys[index * 2];
                }
                size /= 2;
                stride = Math.multiplyExact(stride, 2L);
            }
        } catch (ArithmeticException overflow) {
            throw new IndexException("Run summary count overflow.", overflow);
        }
        selectedPending = (ordinal & (stride - 1)) == 0L;
        return selectedPending;
    }

    /**
     * Supplies the logical natural-order key requested by selectNext.
     *
     * @param key selected logical key, not a rank or encoded representation
     */
    void addSelectedKey(final long key) {
        Vldtn.requireTrue(selectedPending, "No summary key was requested");
        Vldtn.requireTrue(size == 0 || key > keys[size - 1],
                "Selected summary keys must be strictly increasing");
        keys[size++] = key;
        selectedPending = false;
    }

    /** @return immutable weighted snapshot of all emitted output records */
    SenkuLongKeySummary snapshot() {
        Vldtn.requireTrue(!selectedPending,
                "A selected summary key is missing");
        final long[] weights = new long[size];
        Arrays.fill(weights, stride);
        if (size > 0) {
            weights[size - 1] = count - (size - 1L) * stride;
        }
        return SenkuLongKeySummary.of(count, Arrays.copyOf(keys, size),
                weights);
    }
}
