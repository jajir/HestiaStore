package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.datatype.NullValue.NULL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.senku.SenkuMergeFunctions;
import org.junit.jupiter.api.Test;

class SenkuLongSetMapTest {

    @Test
    void traversalRetainsConfiguredHashesAcrossResizeWithoutRerouting() {
        final SenkuLongSetMap map = new SenkuLongSetMap(4, key -> {
            throw new AssertionError("Stored hash must be reused");
        });
        final Map<Long, Integer> expected = new HashMap<>();
        for (long key = -300; key <= 300; key++) {
            final int hash = key % 2 == 0 ? Integer.MIN_VALUE : (int) key;
            expected.put(key, hash);
            assertTrue(map.addLong(key, hash));
        }
        final Map<Long, Integer> actual = new HashMap<>();
        map.forEachLongWithHash(actual::put);
        assertEquals(expected, actual);
        expected.forEach(
                (key, hash) -> assertSame(NULL, map.getWithHash(key, hash)));
        assertThrows(IllegalArgumentException.class,
                () -> map.forEachLongWithHash(null));
        assertThrows(ConcurrentModificationException.class, () -> map
                .forEachLongWithHash((key, hash) -> map.addLong(1000L, 1000)));
    }

    @Test
    void zeroSignedExtremesAndCollisionsSurviveResizesWithoutFalsePositives() {
        final SenkuLongSetMap map = new SenkuLongSetMap(4, key -> 0);
        final Set<Long> expected = new HashSet<>();
        expected.add(Long.MIN_VALUE);
        expected.add(Long.MAX_VALUE);
        for (long key = -500; key <= 500; key++) {
            expected.add(key);
        }
        for (final long key : expected) {
            assertTrue(map.addLong(key, 0));
            assertFalse(map.addLong(key, 0));
        }
        final Set<Long> actual = new HashSet<>();
        map.forEachLong(actual::add);
        assertEquals(expected, actual);
        assertEquals(expected.size(), map.size());
        assertNull(map.get(501L));
        assertTrue(map.isLongSet());
        for (final long key : expected) {
            assertSame(NULL, map.get(key));
        }
    }

    @Test
    void precomputedHashAndPrimitiveTraversalDoNotCallGenericHash() {
        final SenkuLongSetMap map = new SenkuLongSetMap(4, key -> {
            throw new AssertionError("Hash is already computed");
        });
        map.addLong(0L, 7);
        map.addLong(Long.MAX_VALUE, 9);
        final Set<Long> observed = new HashSet<>();
        map.forEachLong(observed::add);
        assertEquals(Set.of(0L, Long.MAX_VALUE), observed);
        assertSame(NULL, map.getWithHash(0L, 7));
    }

    @Test
    void genericViewsRemainCompatibleWithExplicitPureSetSemantics() {
        final SenkuLongSetMap map = new SenkuLongSetMap(4, Long::hashCode);
        assertNull(map.get(0L));
        assertNull(map.put(0L, NULL));
        assertSame(NULL, map.put(0L, NULL));
        assertTrue(map.mergeWithHash(1L, NULL, Long.hashCode(1L),
                SenkuMergeFunctions.longSet()));
        assertFalse(map.mergeWithHash(1L, NULL, Long.hashCode(1L),
                SenkuMergeFunctions.longSet()));
        final Map<Long, NullValue> observed = new HashMap<>();
        map.forEach(observed::put);
        assertEquals(Map.of(0L, NULL, 1L, NULL), observed);
        assertEquals(observed.entrySet(), map.entrySet());
        assertEquals(2, map.entrySet().size());
        assertNull(map.get(null));
        assertNull(map.get("not a long"));
        assertThrows(IllegalArgumentException.class, () -> map.put(null, NULL));
        assertThrows(IllegalArgumentException.class, () -> map.put(0L, null));
        assertThrows(IllegalArgumentException.class,
                () -> map.put(0L, NullValue.TOMBSTONE));
        assertThrows(IllegalArgumentException.class, () -> map.mergeWithHash(0L,
                NULL, 0, (key, first, second) -> NULL));
    }

    @Test
    void traversalAndIteratorDetectStructuralChanges() {
        final SenkuLongSetMap map = new SenkuLongSetMap(4, Long::hashCode);
        final Iterator<Map.Entry<Long, NullValue>> empty = map.entrySet()
                .iterator();
        assertFalse(empty.hasNext());
        assertThrows(NoSuchElementException.class, empty::next);
        map.addLong(0L, 0);
        final Iterator<Map.Entry<Long, NullValue>> iterator = map.entrySet()
                .iterator();
        assertTrue(iterator.hasNext());
        assertThrows(UnsupportedOperationException.class, iterator::remove);
        final AtomicBoolean inserted = new AtomicBoolean();
        assertThrows(ConcurrentModificationException.class,
                () -> map.forEachLong(key -> {
                    if (inserted.compareAndSet(false, true)) {
                        map.addLong(1L, 1);
                    }
                }));
        assertThrows(ConcurrentModificationException.class, iterator::hasNext);
        assertThrows(IllegalArgumentException.class,
                () -> map.forEachLong(null));
        assertThrows(IllegalArgumentException.class, () -> map.forEach(null));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuLongSetMap(0, Long::hashCode));
        assertThrows(IllegalArgumentException.class,
                () -> new SenkuLongSetMap(4, null));
    }
}
