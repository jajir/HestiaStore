package org.hestiastore.index.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.hestiastore.index.Entry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class UniqueCacheTest {

    private final Logger logger = LoggerFactory
            .getLogger(UniqueCacheTest.class);

    private UniqueCache<Integer, String> cache;

    @BeforeEach
    void setup() {
        cache = new UniqueCache<>((i1, i2) -> i1 - i2);
    }

    @AfterEach
    void tearDown() {
        cache = null;
    }

    @Test
    void test_basic_function() {
        cache.put(Entry.of(10, "hello"));
        cache.put(Entry.of(13, "my"));
        cache.put(Entry.of(15, "dear"));

        final List<Entry<Integer, String>> out = cache.toList();
        assertEquals(3, cache.size());
        assertEquals(Entry.of(10, "hello"), out.get(0));
        assertEquals(Entry.of(13, "my"), out.get(1));
        assertEquals(Entry.of(15, "dear"), out.get(2));
        cache.clear();
        assertEquals(0, cache.size());
    }

    @Test
    void test_basic_function_different_order() {
        cache.put(Entry.of(15, "dear"));
        cache.put(Entry.of(13, "my"));
        cache.put(Entry.of(-199, "hello"));

        final List<Entry<Integer, String>> out = cache.getAsSortedList();
        assertEquals(3, cache.size());
        assertEquals(Entry.of(-199, "hello"), out.get(0));
        assertEquals(Entry.of(13, "my"), out.get(1));
        assertEquals(Entry.of(15, "dear"), out.get(2));
        cache.clear();
        assertEquals(0, cache.size());
    }

    /**
     * Test verify that stream is not sorted.
     * 
     * @
     */
    @Test
    void test_stream_sorting() {
        cache.put(Entry.of(15, "dear"));
        cache.put(Entry.of(13, "my"));
        cache.put(Entry.of(-199, "hello"));
        cache.put(Entry.of(-19, "Duck"));

        final List<Entry<Integer, String>> out = cache.getAsSortedList();
        assertEquals(4, cache.size());
        assertEquals(Entry.of(-199, "hello"), out.get(0));
        assertEquals(Entry.of(-19, "Duck"), out.get(1));
        assertEquals(Entry.of(13, "my"), out.get(2));
        assertEquals(Entry.of(15, "dear"), out.get(3));
        assertEquals(4, cache.size());
    }

    /**
     * Verify that merging is called in right time.
     * 
     * @
     */
    @Test
    void test_just_last_value_is_stored() {
        logger.debug("Cache size '{}'", cache.size());
        cache.put(Entry.of(10, "hello"));
        cache.put(Entry.of(10, "my"));
        cache.put(Entry.of(10, "dear"));

        logger.debug("Cache size '{}'", cache.size());
        final List<Entry<Integer, String>> out = cache.toList();
        assertEquals(1, cache.size());
        assertEquals(Entry.of(10, "dear"), out.get(0));
        cache.clear();
        assertEquals(0, cache.size());
    }

}
