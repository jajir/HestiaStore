package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.Pair;
import com.coroptis.index.cache.UniqueCache;

@ExtendWith(MockitoExtension.class)
public class SegmentDeltaCachePairReaderTest {

    @Mock
    private UniqueCache<String, String> cache;

    private SegmentDeltaCachePairReader<String, String> reader;

    @Test
    public void test_read_multiple_values() {
        final List<String> keys = Arrays.asList("key1", "key2", "key3");
        when(cache.getSortedKeys()).thenReturn(keys);
        when(cache.get("key1")).thenReturn("value1");
        when(cache.get("key2")).thenReturn("value2");
        when(cache.get("key3")).thenReturn("value3");
        reader = new SegmentDeltaCachePairReader<>(cache);

        assertEquals(Pair.of("key1", "value1"), reader.read());
        assertEquals(Pair.of("key2", "value2"), reader.read());
        assertEquals(Pair.of("key3", "value3"), reader.read());
        assertNull(reader.read());
    }


    @Test
    public void test_close() {
        final List<String> keys = Arrays.asList("key1", "key2", "key3");
        when(cache.getSortedKeys()).thenReturn(keys);
        when(cache.get("key1")).thenReturn("value1");
        reader = new SegmentDeltaCachePairReader<>(cache);

        assertEquals(Pair.of("key1", "value1"), reader.read());
        reader.close();
        assertNull(reader.read());
        assertNull(reader.read());
        assertNull(reader.read());
        assertNull(reader.read());
        /* Verify that multiple read() on closed reader return just nulls. */
    }

    @SuppressWarnings("resource")
    @Test
    public void test_constructor_cache_is_null() {
        assertThrows(NullPointerException.class, () -> {
            new SegmentDeltaCachePairReader<>(null);
        });
    }
}