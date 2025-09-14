package org.hestiastore.index.chunkpairfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.NoSuchElementException;
import java.util.function.Function;

import org.hestiastore.index.PairIteratorWithCurrent;
import org.hestiastore.index.TestData;
import org.hestiastore.index.chunkstore.Chunk;
import org.hestiastore.index.chunkstore.ChunkStoreReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ChunkPairFileIteratorTest {

    @Mock
    private ChunkStoreReader chunkStoreReader;

    @Mock
    private Function<Chunk, PairIteratorWithCurrent<Integer, String>> iteratorFactory;

    @Mock
    private PairIteratorWithCurrent<Integer, String> chunkIterator1;

    @Mock
    private PairIteratorWithCurrent<Integer, String> chunkIterator2;

    private ChunkPairFileIterator<Integer, String> iterator;

    @AfterEach
    void afterEach() {
        iterator = null;
    }

    private ChunkPairFileIterator<Integer, String> makeIterator() {
        return new ChunkPairFileIterator<>(chunkStoreReader, iteratorFactory);
    }

    @Test
    void test_with_one_chunk() {
        when(chunkStoreReader.read()).thenReturn(TestData.CHUNK_154)
                .thenReturn(null);
        when(iteratorFactory.apply(TestData.CHUNK_154))
                .thenReturn(chunkIterator1).thenReturn(null);

        when(chunkIterator1.hasNext()).thenReturn(true, true, true, false);
        when(chunkIterator1.next()).thenReturn(TestData.PAIR1)
                .thenReturn(TestData.PAIR2).thenReturn(TestData.PAIR3);
        iterator = makeIterator();

        assertTrue(iterator.hasNext());
        assertEquals(TestData.PAIR1, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(TestData.PAIR2, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(TestData.PAIR3, iterator.next());
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
        when(chunkIterator1.next()).thenReturn(TestData.PAIR1)
                .thenReturn(TestData.PAIR2).thenReturn(TestData.PAIR3);
        when(chunkIterator2.hasNext()).thenReturn(true, true, false);
        when(chunkIterator2.next()).thenReturn(TestData.PAIR4)
                .thenReturn(TestData.PAIR5);
        iterator = makeIterator();

        assertTrue(iterator.hasNext());
        assertEquals(TestData.PAIR1, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(TestData.PAIR2, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(TestData.PAIR3, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(TestData.PAIR4, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(TestData.PAIR5, iterator.next());
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
