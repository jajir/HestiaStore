package com.coroptis.index.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheLruTest {

    private final Logger logger = LoggerFactory.getLogger(CacheLruTest.class);

    @Test
    public void test_element_underLimit() throws Exception {
        Cache<Integer, String> cache = new CacheLru<>(5, (k, v) -> {
            // do nothing
            fail();
        });
        cache.put(1, "a");
        cache.put(2, "b");
        cache.put(3, "c");

        assertEquals("c", cache.get(3).get());
        assertEquals("b", cache.get(2).get());
        assertEquals("a", cache.get(1).get());
        assertTrue(cache.get(-2233).isEmpty());
    }

    @Test
    public void test_fill() throws Exception {
        Cache<Integer, String> cache = new CacheLru<>(2, (k, v) -> {
            logger.debug("Removing cached element <'{}','{}'>", k, v);
        });
        cache.put(1, "a");
        cache.put(2, "b");
        assertEquals("a", cache.get(1).get());
        cache.put(3, "c");

        assertEquals("c", cache.get(3).get());
        assertEquals("a", cache.get(1).get());
        assertTrue(cache.get(2).isEmpty());
    }


    @Test
    public void test_invalidate_element() throws Exception {
        Cache<Integer, String> cache = new CacheLru<>(5, (k, v) -> {
            // do nothing
            fail();
        });
        cache.put(1, "a");
        cache.put(2, "b");
        cache.put(3, "c");

        assertEquals("b", cache.get(2).get());
        cache.ivalidate(2);
        assertTrue(cache.get(2).isEmpty());
    }

}
