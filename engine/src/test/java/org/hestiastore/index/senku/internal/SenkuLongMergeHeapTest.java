package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.PriorityQueue;
import java.util.Random;

import org.junit.jupiter.api.Test;

class SenkuLongMergeHeapTest {

    @Test
    void signedExtremesAndTiesUseStableSourceOrderWithoutSentinels() {
        final SenkuLongMergeHeap heap = new SenkuLongMergeHeap(5);
        heap.add(Long.MAX_VALUE, 4);
        heap.add(0L, 3);
        heap.add(Long.MIN_VALUE, 2);
        heap.add(Long.MIN_VALUE, 1);
        heap.add(Long.MIN_VALUE, 0);
        for (int ordinal = 0; ordinal < 3; ordinal++) {
            assertEquals(Long.MIN_VALUE, heap.key());
            assertEquals(ordinal, heap.ordinal());
            heap.removeRoot();
        }
        assertEquals(0L, heap.key());
        assertEquals(3, heap.ordinal());
        heap.removeRoot();
        assertEquals(Long.MAX_VALUE, heap.key());
        heap.replaceRoot(Long.MIN_VALUE);
        assertEquals(Long.MIN_VALUE, heap.key());
        assertEquals(4, heap.ordinal());
        heap.removeRoot();
        assertTrue(heap.isEmpty());
    }

    @Test
    void equalRootReplacementDoesNotSkipEarlierSourceDuplicates() {
        final SenkuLongMergeHeap heap = new SenkuLongMergeHeap(3);
        heap.add(7L, 2);
        heap.add(7L, 1);
        heap.add(7L, 0);
        heap.replaceRoot(7L);
        assertEquals(0, heap.ordinal());
        heap.replaceRoot(8L);
        assertEquals(1, heap.ordinal());
        heap.replaceRoot(Long.MAX_VALUE);
        assertEquals(2, heap.ordinal());
        heap.replaceRoot(Long.MIN_VALUE);
        assertEquals(2, heap.ordinal());
        assertEquals(Long.MIN_VALUE, heap.key());
    }

    @Test
    void randomizedRootUpdatesAndExhaustionMatchPriorityQueue() {
        final Random random = new Random(0x5e1ec7L);
        for (final int count : new int[] { 1, 2, 3, 4, 7, 8, 16, 31, 64 }) {
            for (int repetition = 0; repetition < 12; repetition++) {
                final long[] keys = new long[count];
                final SenkuLongMergeHeap heap = new SenkuLongMergeHeap(count);
                final PriorityQueue<Integer> oracle = new PriorityQueue<>(
                        (first, second) -> {
                            final int compared = Long.compare(keys[first],
                                    keys[second]);
                            return compared == 0
                                    ? Integer.compare(first, second)
                                    : compared;
                        });
                for (int ordinal = count - 1; ordinal >= 0; ordinal--) {
                    keys[ordinal] = randomKey(random);
                    heap.add(keys[ordinal], ordinal);
                    oracle.add(ordinal);
                }
                for (int operation = 0; !oracle.isEmpty(); operation++) {
                    final int selected = oracle.remove();
                    assertFalse(heap.isEmpty());
                    assertEquals(selected, heap.ordinal());
                    assertEquals(keys[selected], heap.key());
                    if (operation >= 1_000 || random.nextInt(20) == 0) {
                        heap.removeRoot();
                    } else {
                        keys[selected] = randomKey(random);
                        heap.replaceRoot(keys[selected]);
                        oracle.add(selected);
                    }
                }
                assertTrue(heap.isEmpty());
            }
        }
    }

    @Test
    void emptyAndClearedSelectorsRejectRootAccessAndCanBeRepopulated() {
        final SenkuLongMergeHeap empty = new SenkuLongMergeHeap(0);
        assertTrue(empty.isEmpty());
        assertThrows(IllegalStateException.class, empty::key);
        assertThrows(IllegalStateException.class, empty::ordinal);
        assertThrows(IllegalStateException.class, empty::removeRoot);
        assertThrows(IllegalStateException.class, () -> empty.replaceRoot(0));
        empty.clear();
        assertTrue(empty.isEmpty());

        final SenkuLongMergeHeap heap = new SenkuLongMergeHeap(2);
        heap.add(1L, 1);
        heap.add(0L, 0);
        heap.clear();
        assertTrue(heap.isEmpty());
        heap.add(Long.MAX_VALUE, 1);
        assertEquals(Long.MAX_VALUE, heap.key());
        assertEquals(1, heap.ordinal());
    }

    @Test
    void invalidCapacityOrdinalAndOverflowDoNotChangeExistingRoot() {
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuLongMergeHeap(-1));
        final SenkuLongMergeHeap heap = new SenkuLongMergeHeap(1);
        assertThrows(IllegalArgumentException.class, () -> heap.add(0L, -1));
        assertThrows(IllegalArgumentException.class, () -> heap.add(0L, 1));
        heap.add(7L, 0);
        assertThrows(IllegalArgumentException.class, () -> heap.add(-1L, 0));
        assertEquals(7L, heap.key());
        assertEquals(0, heap.ordinal());
    }

    private static long randomKey(final Random random) {
        switch (random.nextInt(5)) {
        case 0:
            return Long.MIN_VALUE;
        case 1:
            return Long.MAX_VALUE;
        case 2:
            return random.nextInt(5) - 2L;
        default:
            return random.nextLong();
        }
    }
}
