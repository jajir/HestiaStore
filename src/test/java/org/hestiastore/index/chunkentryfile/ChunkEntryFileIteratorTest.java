package org.hestiastore.index.chunkentryfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.NoSuchElementException;
import java.util.function.Function;

import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.TestData;
import org.hestiastore.index.chunkstore.Chunk;
import org.hestiastore.index.chunkstore.ChunkStoreReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ChunkEntryFileIteratorTest {

    @Mock
    private ChunkStoreReader chunkStoreReader;

    @Mock
    private Function<Chunk, EntryIteratorWithCurrent<Integer, String>> iteratorFactory;

    @Mock
    private EntryIteratorWithCurrent<Integer, String> chunkIterator1;

    @Mock
    private EntryIteratorWithCurrent<Integer, String> chunkIterator2;

    private ChunkEntryFileIterator<Integer, String> iterator;

    @AfterEach
    void afterEach() {
        iterator = null;
    }

    private ChunkEntryFileIterator<Integer, String> makeIterator() {
        return new ChunkEntryFileIterator<>(chunkStoreReader, iteratorFactory);
    }

    @Test
    void test_with_one_chunk() {
        when(chunkStoreReader.read()).thenReturn(TestData.CHUNK_154)
                .thenReturn(null);
        when(iteratorFactory.apply(TestData.CHUNK_154))
                .thenReturn(chunkIterator1).thenReturn(null);

        when(chunkIterator1.hasNext()).thenReturn(true, true, true, false);
        when(chunkIterator1.next()).thenReturn(TestData.ENTRY1)
                .thenReturn(TestData.ENTRY2).thenReturn(TestData.ENTRY3);
        iterator = makeIterator();

        assertTrue(iterator.hasNext());
        assertEquals(TestData.ENTRY1, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(TestData.ENTRY2, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(TestData.ENTRY3, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    void test_with_two_chunks() {
        when(chunkStoreReader.read()).thenReturn(TestData.CHUNK_154)
                .thenReturn(TestData.CHUNK_15).thenReturn(null);
        when(iteratorFactory.apply(TestData.CHUNK_154))
                .thenReturn(chunkIterator1);
        when(iteratorFactory.apply(TestData.CHUNK_15))
                .thenReturn(chunkIterator2);

        when(chunkIterator1.hasNext()).thenReturn(true, true, true, false);
        when(chunkIterator1.next()).thenReturn(TestData.ENTRY1)
                .thenReturn(TestData.ENTRY2).thenReturn(TestData.ENTRY3);
        when(chunkIterator2.hasNext()).thenReturn(true, true, false);
        when(chunkIterator2.next()).thenReturn(TestData.ENTRY4)
                .thenReturn(TestData.ENTRY5);
        iterator = makeIterator();

        assertTrue(iterator.hasNext());
        assertEquals(TestData.ENTRY1, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(TestData.ENTRY2, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(TestData.ENTRY3, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(TestData.ENTRY4, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(TestData.ENTRY5, iterator.next());
        assertFalse(iterator.hasNext());
        assertFalse(iterator.hasNext());
    }

    @Test
    void test_empty_iterator() {
        iterator = makeIterator();
        assertFalse(iterator.hasNext());
        assertFalse(iterator.hasNext());
    }

    @Test
    void test_empty_iterator_next_exception() {
        iterator = makeIterator();
        assertFalse(iterator.hasNext());
        final Exception e = assertThrows(NoSuchElementException.class,
                () -> iterator.next());

        assertEquals("No more elements", e.getMessage());
    }

}
