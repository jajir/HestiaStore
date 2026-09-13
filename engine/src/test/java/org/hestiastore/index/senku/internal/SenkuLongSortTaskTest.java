package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

class SenkuLongSortTaskTest {

    private static final int LARGE_COUNT = 2 * SenkuLongSortTask.LEAF_KEYS + 19;

    @Test
    void largeSignedRandomRangeMatchesArraysSortAndPreservesBoundaries() {
        final long[] keys = new Random(314159L).longs(LARGE_COUNT).toArray();
        keys[1] = Long.MIN_VALUE;
        keys[2] = Long.MAX_VALUE;
        assertSortedLikeJdk(keys, 1, keys.length - 1);
    }

    @Test
    void sortedAndReverseRangesMatchArraysSort() {
        assertSortedLikeJdk(
                IntStream.range(0, LARGE_COUNT)
                        .mapToLong(index -> index - LARGE_COUNT / 2L).toArray(),
                0, LARGE_COUNT);
        assertSortedLikeJdk(
                IntStream.range(0, LARGE_COUNT)
                        .mapToLong(index -> LARGE_COUNT / 2L - index).toArray(),
                0, LARGE_COUNT);
    }

    @Test
    void duplicateHeavyAndEqualRangesMatchArraysSort() {
        assertSortedLikeJdk(
                new Random(271828L).longs(LARGE_COUNT, -3L, 4L).toArray(), 0,
                LARGE_COUNT);
        final long[] equal = new long[LARGE_COUNT];
        Arrays.fill(equal, Long.MIN_VALUE);
        assertSortedLikeJdk(equal, 0, equal.length);
    }

    @Test
    void depthExhaustedLargeSubrangeUsesCorrectHeapFallback() {
        final long[] keys = new Random(161803L)
                .longs(SenkuLongSortTask.LEAF_KEYS + 7).toArray();
        keys[2] = Long.MIN_VALUE;
        keys[3] = Long.MAX_VALUE;
        final long[] expected = keys.clone();
        Arrays.sort(expected, 1, expected.length - 1);
        SenkuFlushExecutor
                .submit(new SenkuLongSortTask(keys, 1, keys.length - 1, 0))
                .join();
        assertArrayEquals(expected, keys);
    }

    @Test
    void emptySingletonAndLeafRangesMatchArraysSort() {
        assertSortedLikeJdk(new long[0], 0, 0);
        assertSortedLikeJdk(new long[] { Long.MAX_VALUE }, 0, 1);
        assertSortedLikeJdk(new long[] { 9L, 3L, -2L, 3L, 7L }, 1, 4);
        assertSortedLikeJdk(
                new Random(42L).longs(SenkuLongSortTask.LEAF_KEYS).toArray(), 0,
                SenkuLongSortTask.LEAF_KEYS);
    }

    @Test
    void invalidRangesAndDepthAreRejectedBeforeSorting() {
        final long[] keys = { 3L, 2L, 1L };
        assertThrows(IllegalArgumentException.class,
                () -> SenkuLongSortTask.sort(null, 0, 0));
        assertThrows(IndexOutOfBoundsException.class,
                () -> SenkuLongSortTask.sort(keys, -1, 2));
        assertThrows(IndexOutOfBoundsException.class,
                () -> SenkuLongSortTask.sort(keys, 0, 4));
        assertThrows(IndexOutOfBoundsException.class,
                () -> new SenkuLongSortTask(keys, 2, 1, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuLongSortTask(keys, 0, 3, -1));
        assertArrayEquals(new long[] { 3L, 2L, 1L }, keys);
    }

    private static void assertSortedLikeJdk(final long[] keys, final int from,
            final int to) {
        final long[] expected = keys.clone();
        Arrays.sort(expected, from, to);
        SenkuLongSortTask.sort(keys, from, to);
        assertArrayEquals(expected, keys);
    }
}
