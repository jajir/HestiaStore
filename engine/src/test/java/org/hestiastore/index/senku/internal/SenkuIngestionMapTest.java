package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
        final SenkuIngestionMap<Integer, Long> countedMap =
                new SenkuIngestionMap<>(4, key -> {
                    invocations.incrementAndGet();
                    return key;
                });

        assertNull(countedMap.putWithHash(1, 2L, 11));
        assertEquals(2L, countedMap.getWithHash(1, 11));
        assertEquals(0, invocations.get());
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
        assertThrows(ConcurrentModificationException.class,
                iterator::hasNext);
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
}
