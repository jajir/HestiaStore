package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.chunkentryfile.ChunkEntryFile;
import org.hestiastore.index.directory.FileReaderSeekable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexSearcherTest {

    @Mock
    private ChunkEntryFile<String, String> chunkEntryFile;

    @Mock
    private EntryIteratorWithCurrent<String, String> iterator;

    @Mock
    private FileReaderSeekable seekableReader;

    private SegmentIndexSearcher<String, String> searcher;

    @BeforeEach
    void setUp() {
        searcher = new SegmentIndexSearcher<>(chunkEntryFile, 3,
                String::compareTo, null);
    }

    @Test
    void search_uses_seekableReader_when_supplier_set() {
        searcher = new SegmentIndexSearcher<>(chunkEntryFile, 3,
                String::compareTo, seekableReader);
        when(chunkEntryFile.openIteratorAtPosition(50L, seekableReader))
                .thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(Entry.of("key", "val"));

        final String out = searcher.search("key", 50L);

        assertEquals("val", out);
        verify(chunkEntryFile, times(1)).openIteratorAtPosition(50L,
                seekableReader);
        verify(iterator, times(1)).close();
    }

    @Test
    void search_without_supplier_uses_default_iterator_and_stops_when_key_greater() {
        when(chunkEntryFile.openIteratorAtPosition(10L)).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(Entry.of("b", "val"));

        final String out = searcher.search("a", 10L);

        assertNull(out);
        verify(chunkEntryFile, times(1)).openIteratorAtPosition(10L);
        verify(chunkEntryFile, never()).openIteratorAtPosition(anyLong(),
                org.mockito.ArgumentMatchers.eq(seekableReader));
        verify(iterator, times(1)).next();
        verify(iterator, times(1)).close();
    }

    @Test
    void search_respects_index_page_limit() {
        searcher = new SegmentIndexSearcher<>(chunkEntryFile, 2,
                String::compareTo, null);
        when(chunkEntryFile.openIteratorAtPosition(0L)).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, true, false);
        when(iterator.next()).thenReturn(Entry.of("a", "v1"))
                .thenReturn(Entry.of("b", "v2")).thenReturn(Entry.of("c", "v3"));

        final String out = searcher.search("c", 0L);

        assertNull(out);
        verify(iterator, times(2)).next();
        verify(iterator, times(1)).close();
    }

    @Test
    void search_finds_value_after_scanning() {
        when(chunkEntryFile.openIteratorAtPosition(5L)).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next()).thenReturn(Entry.of("a", "v1"))
                .thenReturn(Entry.of("b", "v2"));

        final String out = searcher.search("b", 5L);

        assertEquals("v2", out);
        verify(iterator, times(2)).next();
        verify(iterator, times(1)).close();
    }
}
