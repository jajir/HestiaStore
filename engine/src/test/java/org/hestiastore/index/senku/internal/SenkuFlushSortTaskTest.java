package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.junit.jupiter.api.Test;

class SenkuFlushSortTaskTest {

    @Test
    void oversizedSingleShardMatchesSerialOrder() {
        final int count = 2 * SenkuLongSortTask.LEAF_KEYS + 11;
        assertLargeShardSort(new int[] { 0 }, new int[] { count }, count);
    }

    @Test
    void oversizedSkewedShardWithEmptyNeighborsMatchesSerialOrder() {
        final int count = 2 * SenkuLongSortTask.LEAF_KEYS + 11;
        assertLargeShardSort(new int[] { 0, 0, 2, count - 1, count - 1 },
                new int[] { 0, 2, count - 3, 0, 1 }, count);
    }

    @Test
    void parallelSortOwnsDisjointShardRanges() {
        final SenkuFlushOrder<Long, Integer> order = new SenkuFlushOrder<>(
                new TypeDescriptorLong(), new TypeDescriptorInteger(), 8);
        final long[] keys = { 3L, 1L, 2L, 13L, 10L, 12L, 11L, 20L };
        for (int index = 0; index < keys.length; index++) {
            order.set(index, keys[index], index);
        }

        SenkuFlushSortTask.sortAll(order, new int[] { 0, 3, 7 },
                new int[] { 3, 4, 1 }, true);

        assertEquals(1L, order.longKey(0));
        assertEquals(2L, order.longKey(1));
        assertEquals(3L, order.longKey(2));
        assertEquals(10L, order.longKey(3));
        assertEquals(11L, order.longKey(4));
        assertEquals(12L, order.longKey(5));
        assertEquals(13L, order.longKey(6));
        assertEquals(20L, order.longKey(7));
    }

    @Test
    void sorterParallelismHasHardUpperBound() {
        assertTrue(SenkuFlushSortTask.parallelism() >= 1);
        assertTrue(SenkuFlushSortTask.parallelism() <= 4);
    }

    @Test
    void concurrentFlushOrdersRemainIndependent()
            throws InterruptedException, ExecutionException {
        final int jobCount = 8;
        final ExecutorService callers = Executors.newFixedThreadPool(jobCount);
        try {
            final List<Future<Long>> results = new ArrayList<>(jobCount);
            for (int job = 0; job < jobCount; job++) {
                final long offset = 1_000L * job;
                results.add(callers.submit(() -> sortedChecksum(offset)));
            }
            for (int job = 0; job < jobCount; job++) {
                assertEquals(2_005L + 5_000L * job,
                        results.get(job).get().longValue());
            }
        } finally {
            callers.shutdownNow();
        }
    }

    private long sortedChecksum(final long offset) {
        final int entryCount = 1_000;
        final SenkuFlushOrder<Long, Integer> order = new SenkuFlushOrder<>(
                new TypeDescriptorLong(), new TypeDescriptorInteger(),
                entryCount);
        for (int index = 0; index < entryCount; index++) {
            order.set(index, offset + entryCount - index, index);
        }
        final int[] starts = { 0, 200, 400, 600, 800 };
        final int[] counts = { 200, 200, 200, 200, 200 };
        SenkuFlushSortTask.sortAll(order, starts, counts, true);
        long checksum = 0L;
        for (final int start : starts) {
            checksum += order.longKey(start);
        }
        return checksum;
    }

    private static void assertLargeShardSort(final int[] starts,
            final int[] counts, final int count) {
        final long[] expected = new Random(37L).longs(count).toArray();
        final SenkuFlushOrder<Long, NullValue> order = new SenkuFlushOrder<>(
                new TypeDescriptorLong(), new TypeDescriptorNull(), count);
        for (int index = 0; index < count; index++) {
            order.setLong(index, expected[index]);
        }
        for (int shard = 0; shard < starts.length; shard++) {
            Arrays.sort(expected, starts[shard], starts[shard] + counts[shard]);
        }

        SenkuFlushSortTask.sortAll(order, starts, counts, true);

        final long[] actual = new long[count];
        for (int index = 0; index < count; index++) {
            actual[index] = order.longKey(index);
        }
        assertArrayEquals(expected, actual);
    }
}
