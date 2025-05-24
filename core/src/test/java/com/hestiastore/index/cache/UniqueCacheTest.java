package com.hestiastore.index.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hestiastore.index.Pair;

public class UniqueCacheTest {

    private final Logger logger = LoggerFactory
            .getLogger(UniqueCacheTest.class);

    final UniqueCache<Integer, String> cache = new UniqueCache<>(
            (i1, i2) -> i1 - i2);

    @Test
    public void test_basic_function() throws Exception {
        final UniqueCache<Integer, String> cache = new UniqueCache<>(
                (i1, i2) -> i1 - i2);
        cache.put(Pair.of(10, "hello"));
        cache.put(Pair.of(13, "my"));
        cache.put(Pair.of(15, "dear"));

        final List<Pair<Integer, String>> out = cache.toList();
        assertEquals(3, cache.size());
        assertEquals(Pair.of(10, "hello"), out.remove(0));
        assertEquals(Pair.of(13, "my"), out.remove(0));
        assertEquals(Pair.of(15, "dear"), out.remove(0));
        cache.clear();
        assertEquals(0, cache.size());
    }

    @Test
    public void test_basic_function_different_order() throws Exception {
        final UniqueCache<Integer, String> cache = new UniqueCache<>(
                (i1, i2) -> i1 - i2);
        cache.put(Pair.of(15, "dear"));
        cache.put(Pair.of(13, "my"));
        cache.put(Pair.of(-199, "hello"));

        final List<Pair<Integer, String>> out = cache.getAsSortedList();
        assertEquals(3, cache.size());
        assertEquals(Pair.of(-199, "hello"), out.remove(0));
        assertEquals(Pair.of(13, "my"), out.remove(0));
        assertEquals(Pair.of(15, "dear"), out.remove(0));
        cache.clear();
        assertEquals(0, cache.size());
    }

    /**
     * Test verify that stream is not sorted.
     * 
     * @throws Exception
     */
    @Test
    public void test_stream_sorting() throws Exception {
        final UniqueCache<Integer, String> cache = new UniqueCache<>(
                (i1, i2) -> i1 - i2);
        cache.put(Pair.of(15, "dear"));
        cache.put(Pair.of(13, "my"));
        cache.put(Pair.of(-199, "hello"));
        cache.put(Pair.of(-19, "Duck"));

        final List<Pair<Integer, String>> out = cache.getAsSortedList();
        assertEquals(4, cache.size());
        assertEquals(Pair.of(-199, "hello"), out.remove(0));
        assertEquals(Pair.of(-19, "Duck"), out.remove(0));
        assertEquals(Pair.of(13, "my"), out.remove(0));
        assertEquals(Pair.of(15, "dear"), out.remove(0));
        assertEquals(4, cache.size());
    }

    /**
     * Verify that merging is called in right time.
     * 
     * @throws Exception
     */
    @Test
    public void test_just_last_value_is_stored() throws Exception {
        final UniqueCache<Integer, String> cache = new UniqueCache<>(
                (i1, i2) -> i1 - i2);
        logger.debug("Cache size '{}'", cache.size());
        cache.put(Pair.of(10, "hello"));
        cache.put(Pair.of(10, "my"));
        cache.put(Pair.of(10, "dear"));

        logger.debug("Cache size '{}'", cache.size());
        final List<Pair<Integer, String>> out = cache.toList();
        assertEquals(1, cache.size());
        assertEquals(Pair.of(10, "dear"), out.remove(0));
        cache.clear();
        assertEquals(0, cache.size());
    }

}
