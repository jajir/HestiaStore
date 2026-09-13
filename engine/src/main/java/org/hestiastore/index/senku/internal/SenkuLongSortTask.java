package org.hestiastore.index.senku.internal;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.RecursiveAction;

import org.hestiastore.index.Vldtn;

/**
 * In-place, depth-bounded parallel partitioning of one primitive-long range.
 * Three-way partitions remove equal keys from further work; disjoint child
 * ranges run only on the shared flush pool and are always joined. No
 * proportional full-shard scratch array is allocated. JDK leaf sorts are
 * limited to {@link #LEAF_KEYS} keys; a depth-exhausted large range uses an
 * in-place heap sort instead of allocating a large fallback buffer.
 */
final class SenkuLongSortTask extends RecursiveAction {

    private static final long serialVersionUID = 1L;
    static final int LEAF_KEYS = 262_144;

    private final long[] keys;
    private final int from;
    private final int to;
    private final int remainingDepth;

    /** Creates a task owning one valid half-open range and partition budget. */
    SenkuLongSortTask(final long[] keys, final int from, final int to,
            final int remainingDepth) {
        this.keys = Vldtn.requireNonNull(keys, "keys");
        Objects.checkFromToIndex(from, to, keys.length);
        this.from = from;
        this.to = to;
        this.remainingDepth = Vldtn.requireGreaterThanOrEqualToZero(
                remainingDepth, "remainingDepth");
    }

    /**
     * Sorts a range using the existing shared pool, never the common pool. The
     * complete task tree has finished before this method returns or fails.
     */
    static void sort(final long[] keys, final int from, final int to) {
        Vldtn.requireNonNull(keys, "keys");
        Objects.checkFromToIndex(from, to, keys.length);
        final int length = to - from;
        if (length <= LEAF_KEYS || SenkuFlushExecutor.parallelism() == 1) {
            Arrays.sort(keys, from, to);
            return;
        }
        final int depth = 2
                * (Integer.SIZE - 1 - Integer.numberOfLeadingZeros(length));
        SenkuFlushExecutor.submit(new SenkuLongSortTask(keys, from, to, depth))
                .join();
    }

    @Override
    protected void compute() {
        if (to - from <= LEAF_KEYS) {
            Arrays.sort(keys, from, to);
            return;
        }
        if (remainingDepth == 0) {
            heapSort();
            return;
        }
        final long pivot = median(keys[from], keys[from + (to - from) / 2],
                keys[to - 1]);
        int lower = from;
        int current = from;
        int upper = to;
        while (current < upper) {
            final long key = keys[current];
            if (key < pivot) {
                swap(lower++, current++);
            } else if (key > pivot) {
                swap(current, --upper);
            } else {
                current++;
            }
        }
        final SenkuLongSortTask left = new SenkuLongSortTask(keys, from, lower,
                remainingDepth - 1);
        final SenkuLongSortTask right = new SenkuLongSortTask(keys, upper, to,
                remainingDepth - 1);
        SenkuFlushExecutor.invokeSortPair(left, right);
    }

    private void heapSort() {
        final int length = to - from;
        for (int root = length / 2 - 1; root >= 0; root--) {
            siftDown(root, length);
        }
        for (int end = length - 1; end > 0; end--) {
            swap(from, from + end);
            siftDown(0, end);
        }
    }

    private void siftDown(final int initialRoot, final int length) {
        int root = initialRoot;
        while (root < length / 2) {
            int child = 2 * root + 1;
            if (child + 1 < length
                    && keys[from + child] < keys[from + child + 1]) {
                child++;
            }
            if (keys[from + root] >= keys[from + child]) {
                return;
            }
            swap(from + root, from + child);
            root = child;
        }
    }

    private void swap(final int first, final int second) {
        final long key = keys[first];
        keys[first] = keys[second];
        keys[second] = key;
    }

    private static long median(final long first, final long middle,
            final long last) {
        if (first < middle) {
            return middle < last ? middle : Math.max(first, last);
        }
        return first < last ? first : Math.max(middle, last);
    }
}
