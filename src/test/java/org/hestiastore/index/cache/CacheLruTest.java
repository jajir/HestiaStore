package org.hestiastore.index.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.verify;

import java.util.function.BiConsumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
class CacheLruTest {

    private final Logger logger = LoggerFactory.getLogger(CacheLruTest.class);

    private final BiConsumer<Integer, CacheElement> evictedElement = (k, v) -> {
        v.invalidate();
        logger.debug("Removing cached element <'{}','{}'>", k, v);
    };

    @Mock
    private CacheElement value1;

    @Mock
    private CacheElement value2;

    @Mock
    private CacheElement value3;

    @Mock
    private CacheElement value4;

    @Test
    void test_basic_operations() {
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
    void test_remove_size_exceeding_element() {
        Cache<Integer, CacheElement> cache = new CacheLru<>(2, (k, v) -> {
            v.invalidate();
            logger.debug("Removing cached element <'{}','{}'>", k, v);
        });
        cache.put(1, value1);
        cache.put(2, value2);
        assertEquals(value1, cache.get(1).get());
        cache.put(3, value3);

        assertEquals(value3, cache.get(3).get());
        assertEquals(value1, cache.get(1).get());
        assertTrue(cache.get(2).isEmpty());
        verify(value2).invalidate();
    }

    @Test
    void test_invalidate_one_element() {
        Cache<Integer, CacheElement> cache = new CacheLru<>(5, (k, v) -> {
            // this
            v.invalidate();
        });
        cache.put(2, value1);

        assertEquals(value1, cache.get(2).get());
        cache.ivalidate(2);
        assertTrue(cache.get(2).isEmpty());
        verify(value1).invalidate();
    }

    @Test
    void test_invalidateAll() {
        Cache<Integer, CacheElement> cache = new CacheLru<>(2, evictedElement);
        cache.put(1, value1);
        cache.put(2, value2);
        cache.invalidateAll();
        assertTrue(cache.get(1).isEmpty());
        assertTrue(cache.get(2).isEmpty());
        verify(value1).invalidate();
        verify(value2).invalidate();
    }

    @Test
    void test_constructor_limit_too_high() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new CacheLru<>(((long) Integer.MAX_VALUE) + 2L,
                        evictedElement));

        assertEquals("Limit must be less than 2147483647", e.getMessage());
    }

    @Test
    void test_constructor_limit_too_low() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new CacheLru<>(-1, evictedElement));

        assertEquals("Limit must be greater than 0", e.getMessage());
    }

}
