package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.junit.jupiter.api.Test;

class SegmentCacheTest {

    private final TypeDescriptorInteger keyType = new TypeDescriptorInteger();
    private final TypeDescriptorShortString valueType = new TypeDescriptorShortString();
    private static final int DEFAULT_MAX_BUFFERED = 1000;
    private static final int DEFAULT_MAX_DURING_MAINTENANCE = 1000;
    private static final int DEFAULT_MAX_SEGMENT_CACHE = 1024;

    @Test
    void get_prefers_write_cache_over_delta_cache() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType,
                List.of(Entry.of(1, "old")), DEFAULT_MAX_BUFFERED,
                DEFAULT_MAX_DURING_MAINTENANCE, DEFAULT_MAX_SEGMENT_CACHE);

        cache.putToWriteCache(Entry.of(1, "new"));

        assertEquals("new", cache.get(1));
        assertEquals(1, cache.size());
        assertEquals(List.of(Entry.of(1, "new")), cache.getAsSortedList());
    }

    @Test
    void get_falls_back_to_delta_cache() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType,
                List.of(Entry.of(7, "delta")), DEFAULT_MAX_BUFFERED,
                DEFAULT_MAX_DURING_MAINTENANCE, DEFAULT_MAX_SEGMENT_CACHE);

        assertEquals("delta", cache.get(7));
        assertEquals(1, cache.size());
    }

    @Test
    void size_counts_union_of_both_caches() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType,
                List.of(Entry.of(1, "A"), Entry.of(2, "B")),
                DEFAULT_MAX_BUFFERED, DEFAULT_MAX_DURING_MAINTENANCE,
                DEFAULT_MAX_SEGMENT_CACHE);
        cache.putToWriteCache(Entry.of(2, "B2"));
        cache.putToWriteCache(Entry.of(3, "C"));

        assertEquals(3, cache.size());
    }

    @Test
    void sizeWithoutTombstones_ignores_tombstones_in_merged_view() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType,
                List.of(Entry.of(1, "A"), Entry.of(2,
                        TypeDescriptorShortString.TOMBSTONE_VALUE)),
                DEFAULT_MAX_BUFFERED, DEFAULT_MAX_DURING_MAINTENANCE,
                DEFAULT_MAX_SEGMENT_CACHE);
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
                List.of(Entry.of(5, "E"), Entry.of(1, "A")),
                DEFAULT_MAX_BUFFERED, DEFAULT_MAX_DURING_MAINTENANCE,
                DEFAULT_MAX_SEGMENT_CACHE);
        cache.putToWriteCache(Entry.of(3, "C"));
        cache.putToWriteCache(Entry.of(1, "A2"));

        assertEquals(
                List.of(Entry.of(1, "A2"), Entry.of(3, "C"), Entry.of(5, "E")),
                cache.getAsSortedList());
    }

    @Test
    void evictAll_clears_both_caches() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType, List.of(Entry.of(1, "A")),
                DEFAULT_MAX_BUFFERED, DEFAULT_MAX_DURING_MAINTENANCE,
                DEFAULT_MAX_SEGMENT_CACHE);
        cache.putToWriteCache(Entry.of(2, "B"));

        cache.evictAll();

        assertEquals(0, cache.size());
        assertTrue(cache.getAsSortedList().isEmpty());
        assertNull(cache.get(1));
    }

    @Test
    void put_rejects_null_entry() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType, List.of(),
                DEFAULT_MAX_BUFFERED, DEFAULT_MAX_DURING_MAINTENANCE,
                DEFAULT_MAX_SEGMENT_CACHE);

        assertThrows(IllegalArgumentException.class,
                () -> cache.putToWriteCache(null));
    }

    @Test
    void getNumberOfKeysInWriteCache_tracks_overwrites() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType, List.of(),
                DEFAULT_MAX_BUFFERED, DEFAULT_MAX_DURING_MAINTENANCE,
                DEFAULT_MAX_SEGMENT_CACHE);
        cache.putToWriteCache(Entry.of(1, "A"));
        cache.putToWriteCache(Entry.of(1, "B"));
        cache.putToWriteCache(Entry.of(2, "C"));

        assertEquals(2, cache.getNumberOfKeysInWriteCache());
        assertEquals(2, cache.getNumbberOfKeysInCache());
    }

    @Test
    void freezeWriteCache_on_empty_write_cache_keeps_frozen_empty() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType, List.of(),
                DEFAULT_MAX_BUFFERED, DEFAULT_MAX_DURING_MAINTENANCE,
                DEFAULT_MAX_SEGMENT_CACHE);

        assertEquals(List.of(), cache.freezeWriteCache());
        cache.mergeFrozenWriteCacheToDeltaCache();

        assertFalse(cache.hasFrozenWriteCache());
        assertTrue(cache.getAsSortedList().isEmpty());
        assertEquals(0, cache.size());
    }

    @Test
    void freezeWriteCache_moves_entries_and_exposes_frozen_view() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType, List.of(),
                DEFAULT_MAX_BUFFERED, DEFAULT_MAX_DURING_MAINTENANCE,
                DEFAULT_MAX_SEGMENT_CACHE);
        cache.putToWriteCache(Entry.of(2, "B"));
        cache.putToWriteCache(Entry.of(1, "A"));

        assertEquals(
                List.of(Entry.of(1, "A"), Entry.of(2, "B")),
                cache.freezeWriteCache());

        assertEquals(0, cache.getNumberOfKeysInWriteCache());
        assertTrue(cache.hasFrozenWriteCache());
        assertEquals("A", cache.get(1));
        assertEquals(
                List.of(Entry.of(1, "A"), Entry.of(2, "B")),
                cache.getAsSortedList());
        assertEquals(2, cache.size());
        assertEquals(2, cache.getNumbberOfKeysInCache());
    }

    @Test
    void freezeWriteCache_when_already_frozen_does_not_replace_or_consume_new_writes() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType, List.of(),
                DEFAULT_MAX_BUFFERED, DEFAULT_MAX_DURING_MAINTENANCE,
                DEFAULT_MAX_SEGMENT_CACHE);
        cache.putToWriteCache(Entry.of(1, "A"));
        cache.putToWriteCache(Entry.of(2, "B"));

        assertEquals(
                List.of(Entry.of(1, "A"), Entry.of(2, "B")),
                cache.freezeWriteCache());

        cache.putToWriteCache(Entry.of(3, "C"));

        assertEquals(
                List.of(Entry.of(1, "A"), Entry.of(2, "B")),
                cache.freezeWriteCache());
        assertEquals(1, cache.getNumberOfKeysInWriteCache());
        assertEquals(
                List.of(Entry.of(1, "A"), Entry.of(2, "B"),
                        Entry.of(3, "C")),
                cache.getAsSortedList());
    }

    @Test
    void get_prefers_write_then_frozen_then_delta() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType,
                List.of(Entry.of(1, "delta")), DEFAULT_MAX_BUFFERED,
                DEFAULT_MAX_DURING_MAINTENANCE, DEFAULT_MAX_SEGMENT_CACHE);
        cache.putToWriteCache(Entry.of(1, "frozen"));
        cache.freezeWriteCache();
        cache.putToWriteCache(Entry.of(1, "write"));

        assertEquals("write", cache.get(1));
        assertEquals(List.of(Entry.of(1, "write")), cache.getAsSortedList());
    }

    @Test
    void mergeFrozenWriteCacheToDeltaCache_moves_entries_and_clears_frozen() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType,
                List.of(Entry.of(1, "A")), DEFAULT_MAX_BUFFERED,
                DEFAULT_MAX_DURING_MAINTENANCE, DEFAULT_MAX_SEGMENT_CACHE);
        cache.putToWriteCache(Entry.of(2, "B"));
        cache.freezeWriteCache();
        cache.putToWriteCache(Entry.of(3, "C"));

        cache.mergeFrozenWriteCacheToDeltaCache();

        assertFalse(cache.hasFrozenWriteCache());
        assertEquals("B", cache.get(2));
        assertEquals("C", cache.get(3));
        assertEquals(1, cache.getNumberOfKeysInWriteCache());
        assertEquals(3, cache.size());
    }

    @Test
    void getAsSortedList_includes_frozen_and_write_entries_with_overrides() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType,
                List.of(Entry.of(1, "A"), Entry.of(2, "B")),
                DEFAULT_MAX_BUFFERED, DEFAULT_MAX_DURING_MAINTENANCE,
                DEFAULT_MAX_SEGMENT_CACHE);
        cache.putToWriteCache(Entry.of(2, "B2"));
        cache.putToWriteCache(Entry.of(3, "C"));
        cache.freezeWriteCache();
        cache.putToWriteCache(Entry.of(3, "C2"));
        cache.putToWriteCache(Entry.of(4, "D"));

        assertEquals(4, cache.size());
        assertEquals(
                List.of(Entry.of(1, "A"), Entry.of(2, "B2"),
                        Entry.of(3, "C2"), Entry.of(4, "D")),
                cache.getAsSortedList());
    }

    @Test
    void sizeWithoutTombstones_ignores_tombstones_in_frozen_and_write() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType,
                List.of(Entry.of(1, "A")), DEFAULT_MAX_BUFFERED,
                DEFAULT_MAX_DURING_MAINTENANCE, DEFAULT_MAX_SEGMENT_CACHE);
        cache.putToWriteCache(
                Entry.of(2, TypeDescriptorShortString.TOMBSTONE_VALUE));
        cache.freezeWriteCache();
        cache.putToWriteCache(Entry.of(3, "B"));
        cache.putToWriteCache(
                Entry.of(4, TypeDescriptorShortString.TOMBSTONE_VALUE));

        assertEquals(4, cache.size());
        assertEquals(2, cache.sizeWithoutTombstones());
    }

    @Test
    void put_blocks_when_buffer_limit_is_reached() throws Exception {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType, List.of(), 2, 2, 1024);
        cache.putToWriteCache(Entry.of(1, "A"));
        cache.putToWriteCache(Entry.of(2, "B"));

        final CompletableFuture<Void> blockedPut = CompletableFuture
                .runAsync(() -> cache.putToWriteCache(Entry.of(3, "C")));

        assertFalse(blockedPut.isDone());

        cache.freezeWriteCache();
        cache.mergeFrozenWriteCacheToDeltaCache();

        blockedPut.get(1, TimeUnit.SECONDS);
        assertEquals(3, cache.size());
        assertEquals("C", cache.get(3));
    }

    @Test
    void put_blocks_at_write_cache_limit_even_if_flush_limit_is_higher()
            throws Exception {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType, List.of(), 2, 10, 1024);
        cache.putToWriteCache(Entry.of(1, "A"));
        cache.putToWriteCache(Entry.of(2, "B"));

        final CompletableFuture<Void> blockedPut = CompletableFuture
                .runAsync(() -> cache.putToWriteCache(Entry.of(3, "C")));
        assertFalse(blockedPut.isDone());

        cache.freezeWriteCache();
        cache.mergeFrozenWriteCacheToDeltaCache();
        blockedPut.get(1, TimeUnit.SECONDS);
    }

    @Test
    void tryPut_rejects_when_maintenance_limit_is_reached() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType, List.of(), 3, 4, 1024);
        cache.putToWriteCache(Entry.of(1, "A"));
        cache.putToWriteCache(Entry.of(2, "B"));
        cache.putToWriteCache(Entry.of(3, "C"));

        cache.freezeWriteCache();

        assertTrue(cache.tryPutToWriteCacheWithoutWaiting(Entry.of(4, "D")));
        assertFalse(cache.tryPutToWriteCacheWithoutWaiting(Entry.of(5, "E")));
    }

    @Test
    void constructor_rejects_non_positive_limits() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentCache<>(keyType.getComparator(), valueType,
                        List.of(), 0, 1, DEFAULT_MAX_SEGMENT_CACHE));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentCache<>(keyType.getComparator(), valueType,
                        List.of(), 1, 0, DEFAULT_MAX_SEGMENT_CACHE));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentCache<>(keyType.getComparator(), valueType,
                        List.of(), 1, 1, 0));
    }

    @Test
    void evictAll_clears_frozen_cache() {
        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                keyType.getComparator(), valueType,
                List.of(Entry.of(1, "A")), DEFAULT_MAX_BUFFERED,
                DEFAULT_MAX_DURING_MAINTENANCE, DEFAULT_MAX_SEGMENT_CACHE);
        cache.putToWriteCache(Entry.of(2, "B"));
        cache.freezeWriteCache();
        cache.putToWriteCache(Entry.of(3, "C"));

        cache.evictAll();

        assertEquals(0, cache.size());
        assertFalse(cache.hasFrozenWriteCache());
        assertEquals(0, cache.getNumberOfKeysInWriteCache());
        assertNull(cache.get(1));
    }
}
