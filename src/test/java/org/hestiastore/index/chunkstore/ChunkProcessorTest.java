package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.bytes.Bytes;
import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class ChunkProcessorTest {

    private static final ChunkData SAMPLE_DATA = ChunkData.of(0L, 0L,
            ChunkHeader.MAGIC_NUMBER, 1, Bytes.of(new byte[] { 1, 2, 3 }));

    @Test
    void constructor_should_throw_when_filters_null() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new ChunkProcessor(null));

        assertEquals("Property 'filters' must not be null.",
                exception.getMessage());
    }

    @Test
    void constructor_should_throw_when_filters_empty() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new ChunkProcessor(List.of()));

        assertEquals("Property 'filters' must not be empty.",
                exception.getMessage());
    }

    @Test
    void process_should_throw_when_data_null() {
        final ChunkProcessor processor = new ChunkProcessor(
                List.of(mock(ChunkFilter.class)));

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, () -> processor.process(null));

        assertEquals("Property 'data' must not be null.",
                exception.getMessage());
    }

    @Test
    void process_should_propagate_filter_exception() {
        final IndexException expected = new IndexException("boom");
        final ChunkFilter throwingFilter = data -> {
            throw expected;
        };
        final ChunkProcessor processor = new ChunkProcessor(
                List.of(throwingFilter));

        final IndexException exception = assertThrows(IndexException.class,
                () -> processor.process(SAMPLE_DATA));

        assertSame(expected, exception);
    }

    @Test
    void process_should_execute_filters_in_sequence() {
        final ChunkFilter first = mock(ChunkFilter.class);
        final ChunkFilter second = mock(ChunkFilter.class);
        final ChunkData afterFirst = SAMPLE_DATA.withCrc(42L);
        final ChunkData afterSecond = afterFirst.withFlags(7L);
        when(first.apply(SAMPLE_DATA)).thenReturn(afterFirst);
        when(second.apply(afterFirst)).thenReturn(afterSecond);
        final ChunkProcessor processor = new ChunkProcessor(
                List.of(first, second));

        final ChunkData result = processor.process(SAMPLE_DATA);

        assertEquals(afterSecond, result);
        verify(first).apply(SAMPLE_DATA);
        verify(second).apply(afterFirst);
        verifyNoMoreInteractions(first, second);
    }
}
