package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.junit.jupiter.api.Test;

class SegmentCacheTest {

    private final TypeDescriptorInteger keyType = new TypeDescriptorInteger();
    private final TypeDescriptorShortString valueType = new TypeDescriptorShortString();

    @Test
    void get_prefers_write_cache_over_delta_cache() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType,
                List.of(Entry.of(1, "old")));

        cache.putToWriteCache(Entry.of(1, "new"));

        assertEquals("new", cache.get(1));
        assertEquals(1, cache.size());
        assertEquals(List.of(Entry.of(1, "new")), cache.getAsSortedList());
    }

    @Test
    void get_falls_back_to_delta_cache() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType,
                List.of(Entry.of(7, "delta")));

        assertEquals("delta", cache.get(7));
        assertEquals(1, cache.size());
    }

    @Test
    void size_counts_union_of_both_caches() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType,
                List.of(Entry.of(1, "A"), Entry.of(2, "B")));
        cache.putToWriteCache(Entry.of(2, "B2"));
        cache.putToWriteCache(Entry.of(3, "C"));

        assertEquals(3, cache.size());
    }

    @Test
    void sizeWithoutTombstones_ignores_tombstones_in_merged_view() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType,
                List.of(Entry.of(1, "A"), Entry.of(2,
                        TypeDescriptorShortString.TOMBSTONE_VALUE)));
        cache.putToWriteCache(Entry.of(2, "B"));
        cache.putToWriteCache(
                Entry.of(3, TypeDescriptorShortString.TOMBSTONE_VALUE));

        assertEquals(3, cache.size());
        assertEquals(2, cache.sizeWithoutTombstones());
    }

    @Test
    void getAsSortedList_returns_sorted_and_overridden_entries() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType,
                List.of(Entry.of(5, "E"), Entry.of(1, "A")));
        cache.putToWriteCache(Entry.of(3, "C"));
        cache.putToWriteCache(Entry.of(1, "A2"));

        assertEquals(
                List.of(Entry.of(1, "A2"), Entry.of(3, "C"), Entry.of(5, "E")),
                cache.getAsSortedList());
    }

    @Test
    void evictAll_clears_both_caches() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType, List.of(Entry.of(1, "A")));
        cache.putToWriteCache(Entry.of(2, "B"));

        cache.evictAll();

        assertEquals(0, cache.size());
        assertTrue(cache.getAsSortedList().isEmpty());
        assertNull(cache.get(1));
    }

    @Test
    void put_rejects_null_entry() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType);

        assertThrows(IllegalArgumentException.class,
                () -> cache.putToWriteCache(null));
    }

    @Test
    void getWriteCacheAsSortedList_and_clearWriteCache() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType);
        cache.putToWriteCache(Entry.of(3, "C"));
        cache.putToWriteCache(Entry.of(1, "A"));
        cache.putToWriteCache(Entry.of(2, "B"));

        assertEquals(
                List.of(Entry.of(1, "A"), Entry.of(2, "B"), Entry.of(3, "C")),
                cache.getWriteCacheAsSortedList());

        cache.clearWriteCache();
        assertEquals(0, cache.size());
    }

    @Test
    void mergeWriteCacheToDeltaCache_moves_entries_and_clears_write_cache() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType,
                List.of(Entry.of(1, "old")));
        cache.putToWriteCache(Entry.of(1, "new"));
        cache.putToWriteCache(Entry.of(2, "B"));

        cache.mergeWriteCacheToDeltaCache();

        assertEquals("new", cache.get(1));
        assertEquals("B", cache.get(2));
        assertTrue(cache.getWriteCacheAsSortedList().isEmpty());
        assertEquals(2, cache.size());
    }

    @Test
    void mergeWriteCacheToDeltaCache_accepts_empty_snapshot() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType, List.of(Entry.of(1, "A")));

        cache.mergeWriteCacheToDeltaCache();

        assertEquals("A", cache.get(1));
        assertEquals(1, cache.size());
        assertTrue(cache.getWriteCacheAsSortedList().isEmpty());
    }

    @Test
    void getNumberOfKeysInWriteCache_tracks_overwrites() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType);
        cache.putToWriteCache(Entry.of(1, "A"));
        cache.putToWriteCache(Entry.of(1, "B"));
        cache.putToWriteCache(Entry.of(2, "C"));

        assertEquals(2, cache.getNumberOfKeysInWriteCache());
        assertEquals(2, cache.getNumbberOfKeysInCache());
    }
}
