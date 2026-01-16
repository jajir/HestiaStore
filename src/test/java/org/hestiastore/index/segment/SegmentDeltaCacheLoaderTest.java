package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.chunkentryfile.ChunkEntryFile;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.sorteddatafile.SortedDataFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentDeltaCacheLoaderTest {

    @Mock
    private SegmentFiles<Integer, String> segmentFiles;

    @Mock
    private SegmentPropertiesManager propertiesManager;

    @Mock
    private SortedDataFile<Integer, String> cacheFile;

    @Mock
    private ChunkEntryFile<Integer, String> deltaFile1;

    @Mock
    private ChunkEntryFile<Integer, String> deltaFile2;

    private SegmentCache<Integer, String> newCacheWith(
            final List<Entry<Integer, String>> cacheEntries,
            final List<Entry<Integer, String>> delta1,
            final List<Entry<Integer, String>> delta2) {
        final EntryIteratorWithCurrent<Integer, String> itCache = new EntryIteratorList<>(
                cacheEntries);
        final EntryIteratorWithCurrent<Integer, String> itDelta1 = new EntryIteratorList<>(
                delta1);
        final EntryIteratorWithCurrent<Integer, String> itDelta2 = new EntryIteratorList<>(
                delta2);

        when(segmentFiles.getCacheDataFile()).thenReturn(cacheFile);
        when(cacheFile.openIterator()).thenReturn(itCache);

        when(propertiesManager.getCacheDeltaFileNames())
                .thenReturn(List.of("d1", "d2"));
        when(segmentFiles.getDeltaCacheChunkEntryFile("d1"))
                .thenReturn(deltaFile1);
        when(segmentFiles.getDeltaCacheChunkEntryFile("d2"))
                .thenReturn(deltaFile2);
        when(deltaFile1.openIterator()).thenReturn(itDelta1);
        when(deltaFile2.openIterator()).thenReturn(itDelta2);

        final SegmentCache<Integer, String> cache = new SegmentCache<>(
                new TypeDescriptorInteger().getComparator(),
                new TypeDescriptorShortString(), null, 100, 200, 1024);
        new SegmentDeltaCacheLoader<>(segmentFiles, propertiesManager)
                .loadInto(cache);
        return cache;
    }

    @Test
    void loadInto_merges_cache_and_deltas_with_last_value_wins() {
        final SegmentCache<Integer, String> cache = newCacheWith(
                List.of(Entry.of(1, "A"), Entry.of(2, "B")),
                List.of(Entry.of(2, "B2"), Entry.of(3, "C")),
                List.of(Entry.of(1, "A2")));

        assertEquals(3, cache.size());
        assertEquals("A2", cache.get(1));
        assertEquals("B2", cache.get(2));
        assertEquals("C", cache.get(3));

        final List<Entry<Integer, String>> sorted = cache.getAsSortedList();
        assertEquals(
                List.of(Entry.of(1, "A2"), Entry.of(2, "B2"), Entry.of(3, "C")),
                sorted);
    }
}
