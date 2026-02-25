package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Supplier;

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

    @Mock
    private Supplier<FileReaderSeekable> seekableReaderSupplier;

    private SegmentIndexSearcher<String, String> searcher;

    @BeforeEach
    void setUp() {
        searcher = new SegmentIndexSearcher<>(chunkEntryFile, 3,
                String::compareTo, seekableReaderSupplier);
    }

    @Test
    void search_uses_seekable_reader_from_factory() {
        searcher = new SegmentIndexSearcher<>(chunkEntryFile, 3,
                String::compareTo, seekableReaderSupplier);
        when(seekableReaderSupplier.get()).thenReturn(seekableReader);
        when(chunkEntryFile.openIteratorAtPosition(50L, seekableReader))
                .thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(Entry.of("key", "val"));

        final String out = searcher.search("key", 50L);

        assertEquals("val", out);
        verify(seekableReaderSupplier, times(1)).get();
        verify(chunkEntryFile, times(1)).openIteratorAtPosition(50L,
                seekableReader);
        verify(iterator, times(1)).close();
        verify(seekableReader, times(1)).close();
    }

    @Test
    void search_stops_when_key_greater() {
        when(seekableReaderSupplier.get()).thenReturn(seekableReader);
        when(chunkEntryFile.openIteratorAtPosition(10L, seekableReader))
                .thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(Entry.of("b", "val"));

        final String out = searcher.search("a", 10L);

        assertNull(out);
        verify(seekableReaderSupplier, times(1)).get();
        verify(chunkEntryFile, times(1)).openIteratorAtPosition(10L,
                seekableReader);
        verify(iterator, times(1)).next();
        verify(iterator, times(1)).close();
        verify(seekableReader, times(1)).close();
    }

    @Test
    void search_respects_index_page_limit() {
        searcher = new SegmentIndexSearcher<>(chunkEntryFile, 2,
                String::compareTo, seekableReaderSupplier);
        when(seekableReaderSupplier.get()).thenReturn(seekableReader);
        when(chunkEntryFile.openIteratorAtPosition(0L, seekableReader))
                .thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, true, false);
        when(iterator.next()).thenReturn(Entry.of("a", "v1"))
                .thenReturn(Entry.of("b", "v2")).thenReturn(Entry.of("c", "v3"));

        final String out = searcher.search("c", 0L);

        assertNull(out);
        verify(iterator, times(2)).next();
        verify(iterator, times(1)).close();
        verify(seekableReader, times(1)).close();
    }

    @Test
    void search_finds_value_after_scanning() {
        when(seekableReaderSupplier.get()).thenReturn(seekableReader);
        when(chunkEntryFile.openIteratorAtPosition(5L, seekableReader))
                .thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next()).thenReturn(Entry.of("a", "v1"))
                .thenReturn(Entry.of("b", "v2"));

        final String out = searcher.search("b", 5L);

        assertEquals("v2", out);
        verify(iterator, times(2)).next();
        verify(iterator, times(1)).close();
        verify(seekableReader, times(1)).close();
    }

    @Test
    void search_propagates_factory_exception() {
        when(seekableReaderSupplier.get())
                .thenThrow(new IllegalStateException("open failure"));

        final Exception e = assertThrows(IllegalStateException.class,
                () -> searcher.search("a", 5L));
        assertEquals("open failure", e.getMessage());
    }

    @Test
    void constructor_requires_seekable_reader_supplier() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexSearcher<>(chunkEntryFile, 3,
                        String::compareTo, null));
        assertEquals("Property 'seekableReaderSupplier' must not be null.",
                e.getMessage());
    }

}
