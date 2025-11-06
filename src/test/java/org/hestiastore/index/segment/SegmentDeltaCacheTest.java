package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.sorteddatafile.SortedDataFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentDeltaCacheTest {

    @Mock
    private SegmentFiles<Integer, String> segmentFiles;

    @Mock
    private SegmentPropertiesManager propertiesManager;

    @Mock
    private SortedDataFile<Integer, String> cacheFile;

    @Mock
    private SortedDataFile<Integer, String> deltaFile1;

    @Mock
    private SortedDataFile<Integer, String> deltaFile2;

    private SegmentDeltaCache<Integer, String> newCacheWith(
            final List<Entry<Integer, String>> cacheEntries,
            final List<Entry<Integer, String>> delta1,
            final List<Entry<Integer, String>> delta2) {
        final EntryIteratorWithCurrent<Integer, String> itCache = new EntryIteratorList<>(cacheEntries);
        final EntryIteratorWithCurrent<Integer, String> itDelta1 = new EntryIteratorList<>(delta1);
        final EntryIteratorWithCurrent<Integer, String> itDelta2 = new EntryIteratorList<>(delta2);

        when(segmentFiles.getCacheDataFile()).thenReturn(cacheFile);
        when(cacheFile.openIterator()).thenReturn(itCache);

        when(propertiesManager.getCacheDeltaFileNames()).thenReturn(List.of("d1", "d2"));
        when(segmentFiles.getDeltaCacheSortedDataFile("d1")).thenReturn(deltaFile1);
        when(segmentFiles.getDeltaCacheSortedDataFile("d2")).thenReturn(deltaFile2);
        when(deltaFile1.openIterator()).thenReturn(itDelta1);
        when(deltaFile2.openIterator()).thenReturn(itDelta2);

        return new SegmentDeltaCache<>(new TypeDescriptorInteger(), segmentFiles, propertiesManager);
    }

    @Test
    void constructor_loads_from_cache_and_deltas_with_last_value_wins() {
        final SegmentDeltaCache<Integer, String> cache = newCacheWith(
                List.of(Entry.of(1, "A"), Entry.of(2, "B")),
                List.of(Entry.of(2, "B2"), Entry.of(3, "C")),
                List.of(Entry.of(1, "A2")));

        assertEquals(3, cache.size());
        assertEquals("A2", cache.get(1));
        assertEquals("B2", cache.get(2));
        assertEquals("C", cache.get(3));

        // sorted order by Integer comparator
        final List<Entry<Integer, String>> sorted = cache.getAsSortedList();
        assertEquals(List.of(Entry.of(1, "A2"), Entry.of(2, "B2"), Entry.of(3, "C")), sorted);
    }

    @Test
    void put_null_entry_throws() {
        final SegmentDeltaCache<Integer, String> cache = newCacheWith(List.of(), List.of(), List.of());
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> cache.put(null));
        assertEquals("Property 'entry' must not be null.", e.getMessage());
    }

    @Test
    void put_overrides_previous_value_and_updates_size() {
        final SegmentDeltaCache<Integer, String> cache = newCacheWith(
                List.of(Entry.of(5, "X")), List.of(), List.of());

        assertEquals(1, cache.size());
        cache.put(Entry.of(5, "Y"));
        assertEquals(1, cache.size());
        assertEquals("Y", cache.get(5));
    }

    @Test
    void sizeWithoutTombstones_filters_tombstone_values() {
        final TypeDescriptorShortString std = new TypeDescriptorShortString();
        final String TOMBSTONE = std.getTombstone();
        // value TD for tombstone detection used by sizeWithoutTombstones()
        when(segmentFiles.getValueTypeDescriptor()).thenReturn(std);

        // Force segmentFiles to use String TD (already setup in helper)
        final SegmentDeltaCache<Integer, String> cache = newCacheWith(
                List.of(Entry.of(10, "A"), Entry.of(20, TOMBSTONE)),
                List.of(Entry.of(30, "C")),
                List.of(Entry.of(40, TOMBSTONE)));

        assertEquals(4, cache.size());
        assertEquals(2, cache.sizeWithoutTombstones());
    }

    @Test
    void evictAll_clears_cache() {
        final SegmentDeltaCache<Integer, String> cache = newCacheWith(
                List.of(Entry.of(1, "A")), List.of(Entry.of(2, "B")), List.of());
        assertTrue(cache.size() > 0);
        cache.evictAll();
        assertEquals(0, cache.size());
        assertNull(cache.get(1));
    }

    @Test
    void get_with_null_key_throws_from_delegate_cache() {
        final SegmentDeltaCache<Integer, String> cache = newCacheWith(List.of(), List.of(), List.of());
        final Exception e = assertThrows(IllegalArgumentException.class, () -> cache.get(null));
        assertEquals("Property 'key' must not be null.", e.getMessage());
    }
}
