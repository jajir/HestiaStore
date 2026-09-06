package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.IndexException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SenkuIngestionMapTest {

    private SenkuIngestionMap<Integer, Long> map;

    @BeforeEach
    void setUp() {
        map = new SenkuIngestionMap<>(4, value -> value & 31);
    }

    @Test
    void insertLookupAndReplaceUseConfiguredHash() {
        final Integer first = Integer.valueOf(1_000);
        final Integer equal = Integer.valueOf(1_000);

        assertNull(map.put(first, 2L));
        assertEquals(2L, map.get(equal));
        assertEquals(2L, map.put(equal, 3L));
        assertEquals(3L, map.get(first));
        assertEquals(1, map.size());
    }

    @Test
    void precomputedHashAvoidsAnotherHashFunctionInvocation() {
        final AtomicInteger invocations = new AtomicInteger();
        final SenkuIngestionMap<Integer, Long> countedMap = new SenkuIngestionMap<>(
                4, key -> {
                    invocations.incrementAndGet();
                    return key;
                });

        assertNull(countedMap.putWithHash(1, 2L, 11));
        assertEquals(2L, countedMap.getWithHash(1, 11));
        assertEquals(0, invocations.get());
    }

    @Test
    void mergeProbesOnceAndPassesTheIncomingLogicalKey() {
        final SenkuIngestionMap<ProbeKey, Long> counted = new SenkuIngestionMap<>(
                16, key -> {
                    throw new AssertionError("Hash was already computed");
                });
        counted.mergeWithHash(new ProbeKey(1), 1L, 0,
                (key, first, second) -> first + second);
        counted.mergeWithHash(new ProbeKey(2), 2L, 0,
                (key, first, second) -> first + second);
        final ProbeKey inserted = new ProbeKey(3);
        assertTrue(
                counted.mergeWithHash(inserted, 3L, 0, (key, first, second) -> {
                    throw new AssertionError("New key must not reduce");
                }));
        assertEquals(2, inserted.comparisons);
        final ProbeKey incoming = new ProbeKey(3);
        final AtomicReference<ProbeKey> callbackKey = new AtomicReference<>();
        assertFalse(
                counted.mergeWithHash(incoming, 4L, 0, (key, first, second) -> {
                    callbackKey.set(key);
                    return first + second;
                }));
        assertEquals(3, incoming.comparisons);
        assertSame(incoming, callbackKey.get());
        assertEquals(3, counted.size());
        assertEquals(7L, counted.getWithHash(inserted, 0));
    }

    @Test
    void mergeFailuresPreserveOldValueAndIndexExceptionIdentity() {
        map.mergeWithHash(1, 2L, 1, (key, first, second) -> first + second);
        final IndexException failure = new IndexException("expected");
        assertSame(failure, assertThrows(IndexException.class,
                () -> map.mergeWithHash(1, 3L, 1, (key, first, second) -> {
                    throw failure;
                })));
        final IllegalStateException cause = new IllegalStateException(
                "callback");
        assertSame(cause, assertThrows(IndexException.class,
                () -> map.mergeWithHash(1, 3L, 1, (key, first, second) -> {
                    throw cause;
                })).getCause());
        assertThrows(IndexException.class, () -> map.mergeWithHash(1, 3L, 1,
                (key, first, second) -> null));
        assertEquals(2L, map.get(1));
        assertEquals(1, map.size());
        assertFalse(map.isLongSet());
        assertThrows(IllegalStateException.class, () -> map.forEachLong(key -> {
        }));
    }

    @Test
    void singleProbeInsertionResizesAndRetainsEveryValue() {
        for (int key = 0; key < 100; key++) {
            assertTrue(map.mergeWithHash(key, (long) key, 0,
                    (logicalKey, first, second) -> first + second));
        }
        for (int key = 0; key < 100; key++) {
            assertEquals((long) key, map.getWithHash(key, 0));
        }
    }

    @Test
    void resizingRetainsCollisionHeavyMappings() {
        final Map<Integer, Long> expected = new HashMap<>();
        for (int index = 0; index < 10_000; index++) {
            final Integer key = Integer.valueOf(index);
            final Long value = Long.valueOf(index * 3L);
            expected.put(key, value);
            map.put(key, value);
        }

        assertEquals(expected.size(), map.size());
        for (final Map.Entry<Integer, Long> entry : expected.entrySet()) {
            assertEquals(entry.getValue(), map.get(entry.getKey()));
        }
    }

    @Test
    void tableHashRemainsIndependentFromMutationStripeBits() {
        final int[] buckets = new int[1_024];
        int observedBuckets = 0;
        for (int hash = 0; hash < 1_000_000; hash++) {
            if (SenkuIngestor.stripeFromHash(hash) == 0) {
                final int bucket = SenkuIngestionMap.tableHash(hash)
                        & (buckets.length - 1);
                if (buckets[bucket]++ == 0) {
                    observedBuckets++;
                }
            }
        }

        assertTrue(observedBuckets > 1_000,
                "table mixer must not inherit fixed mutation-stripe bits");
    }

    @Test
    void entryIterationAndForEachExposeEveryMapping() {
        final Map<Integer, Long> iterated = new HashMap<>();
        final Map<Integer, Long> consumed = new HashMap<>();
        for (int index = 0; index < 100; index++) {
            map.put(Integer.valueOf(index), Long.valueOf(index));
        }

        for (final Map.Entry<Integer, Long> entry : map.entrySet()) {
            iterated.put(entry.getKey(), entry.getValue());
        }
        map.forEach(consumed::put);

        assertEquals(100, iterated.size());
        assertEquals(iterated, consumed);
    }

    @Test
    void forEachDetectsStructuralModification() {
        map.put(1, 1L);
        final AtomicBoolean inserted = new AtomicBoolean();

        assertThrows(ConcurrentModificationException.class,
                () -> map.forEach((key, value) -> {
                    if (inserted.compareAndSet(false, true)) {
                        map.put(2, 2L);
                    }
                }));
    }

    @Test
    void iteratorIsFailFastAndDoesNotSupportRemoval() {
        map.put(1, 1L);
        final Iterator<Map.Entry<Integer, Long>> iterator = map.entrySet()
                .iterator();

        assertTrue(iterator.hasNext());
        assertThrows(UnsupportedOperationException.class, iterator::remove);
        map.put(2, 2L);
        assertThrows(ConcurrentModificationException.class, iterator::hasNext);
    }

    @Test
    void emptyAndExhaustedIteratorsFollowMapContract() {
        final Iterator<Map.Entry<Integer, Long>> iterator = map.entrySet()
                .iterator();

        assertFalse(iterator.hasNext());
        assertThrows(java.util.NoSuchElementException.class, iterator::next);
    }

    @Test
    void rejectsInvalidConstructionAndNullMappings() {
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuIngestionMap<Integer, Long>(0, value -> value));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuIngestionMap<Integer, Long>(4, null));
        assertThrows(IllegalArgumentException.class, () -> map.put(null, 1L));
        assertThrows(IllegalArgumentException.class, () -> map.put(1, null));
        assertNull(map.get(null));
    }

    @Test
    void tableMixerIsDeterministicForExtremeHashes() {
        final List<Integer> first = mixedExtremeHashes();
        final List<Integer> second = mixedExtremeHashes();

        assertEquals(first, second);
        assertEquals(first.size(), first.stream().distinct().count());
    }

    @Test
    void fullProbeTableFailsInsteadOfLooping() {
        final Object[] keys = { 1, 2, 3, 4 };
        final int[] hashes = { 0, 0, 0, 0 };

        assertThrows(org.hestiastore.index.IndexException.class,
                () -> SenkuIngestionMap.findSlot(5, 0, keys, hashes));
    }

    private static List<Integer> mixedExtremeHashes() {
        final List<Integer> hashes = new ArrayList<>();
        hashes.add(SenkuIngestionMap.tableHash(Integer.MIN_VALUE));
        hashes.add(SenkuIngestionMap.tableHash(-1));
        hashes.add(SenkuIngestionMap.tableHash(0));
        hashes.add(SenkuIngestionMap.tableHash(1));
        hashes.add(SenkuIngestionMap.tableHash(Integer.MAX_VALUE));
        return hashes;
    }

    private static final class ProbeKey {
        private final int value;
        private int comparisons;

        private ProbeKey(final int value) {
            this.value = value;
        }

        @Override
        public boolean equals(final Object other) {
            comparisons++;
            return other instanceof ProbeKey
                    && ((ProbeKey) other).value == value;
        }

        @Override
        public int hashCode() {
            return value;
        }
    }
}
