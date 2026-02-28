package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.chunkentryfile.ChunkEntryFile;
import org.hestiastore.index.directory.FileReaderSeekable;
import org.hestiastore.index.directory.FileReaderSeekableSupplier;
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
    private FileReaderSeekable seekableReader;

    @Mock
    private FileReaderSeekableSupplier seekableReaderSupplier;

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
        when(chunkEntryFile.searchAtPosition(eq("key"), eq(50L), eq(3), any(),
                same(seekableReader))).thenReturn("val");

        final String out = searcher.search("key", 50L);

        assertEquals("val", out);
        verify(seekableReaderSupplier, times(1)).get();
        verify(chunkEntryFile, times(1)).searchAtPosition(eq("key"), eq(50L),
                eq(3), any(), same(seekableReader));
        verify(seekableReader, times(1)).close();
    }

    @Test
    void search_stops_when_key_greater() {
        when(seekableReaderSupplier.get()).thenReturn(seekableReader);
        when(chunkEntryFile.searchAtPosition(eq("a"), eq(10L), eq(3), any(),
                same(seekableReader))).thenReturn(null);

        final String out = searcher.search("a", 10L);

        assertNull(out);
        verify(seekableReaderSupplier, times(1)).get();
        verify(chunkEntryFile, times(1)).searchAtPosition(eq("a"), eq(10L),
                eq(3), any(), same(seekableReader));
        verify(seekableReader, times(1)).close();
    }

    @Test
    void search_respects_index_page_limit() {
        searcher = new SegmentIndexSearcher<>(chunkEntryFile, 2,
                String::compareTo, seekableReaderSupplier);
        when(seekableReaderSupplier.get()).thenReturn(seekableReader);
        when(chunkEntryFile.searchAtPosition(eq("c"), eq(0L), eq(2), any(),
                same(seekableReader))).thenReturn(null);

        final String out = searcher.search("c", 0L);

        assertNull(out);
        verify(chunkEntryFile, times(1)).searchAtPosition(eq("c"), eq(0L),
                eq(2), any(), same(seekableReader));
        verify(seekableReader, times(1)).close();
    }

    @Test
    void search_finds_value_after_scanning() {
        when(seekableReaderSupplier.get()).thenReturn(seekableReader);
        when(chunkEntryFile.searchAtPosition(eq("b"), eq(5L), eq(3), any(),
                same(seekableReader))).thenReturn("v2");

        final String out = searcher.search("b", 5L);

        assertEquals("v2", out);
        verify(chunkEntryFile, times(1)).searchAtPosition(eq("b"), eq(5L),
                eq(3), any(), same(seekableReader));
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

    @Test
    void close_closes_seekable_reader_supplier() {
        searcher.close();

        verify(seekableReaderSupplier, times(1)).close();
    }

}
