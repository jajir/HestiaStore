package org.hestiastore.index.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.function.BiConsumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CacheLruTest {

    @Mock
    private BiConsumer<Integer, String> evictionListener;

    @Test
    void putAndGetReturnStoredValues() {
        Cache<Integer, String> cache = new CacheLru<>(3, failOnEviction());

        cache.put(1, "one");
        cache.put(2, "two");
        cache.put(3, "three");

        assertEquals("three", cache.get(3).get());
        assertEquals("two", cache.get(2).get());
        assertEquals("one", cache.get(1).get());
        assertTrue(cache.get(999).isEmpty());
    }

    @Test
    void evictsLeastRecentlyUsedEntryWhenLimitReached() {
        Cache<Integer, String> cache = new CacheLru<>(2, evictionListener);

        cache.put(1, "one");
        cache.put(2, "two");
        cache.put(3, "three"); // should evict key 1

        assertTrue(cache.get(1).isEmpty());
        assertEquals("two", cache.get(2).get());
        assertEquals("three", cache.get(3).get());
        verify(evictionListener).accept(1, "one");
    }

    @Test
    void getUpdatesRecency() {
        Cache<Integer, String> cache = new CacheLru<>(2, evictionListener);

        cache.put(1, "one");
        cache.put(2, "two");
        cache.get(1); // makes key 1 most recently used
        cache.put(3, "three"); // should evict key 2

        assertTrue(cache.get(2).isEmpty());
        assertEquals("one", cache.get(1).get());
        assertEquals("three", cache.get(3).get());
        verify(evictionListener).accept(2, "two");
    }

    @Test
    void putNullEntryCannotBeRead() {
        CacheLru<Integer, String> cache = new CacheLru<>(1, failOnEviction());

        cache.putNull(1);

        IllegalStateException exception = assertThrows(
                IllegalStateException.class, () -> cache.get(1));
        assertEquals("Null element does not have a value",
                exception.getMessage());
    }

    @Test
    void ivalidateRemovesKeyAndNotifiesListener() {
        Cache<Integer, String> cache = new CacheLru<>(3, evictionListener);

        cache.put(1, "one");
        cache.put(2, "two");
        cache.ivalidate(1);

        assertTrue(cache.get(1).isEmpty());
        assertEquals("two", cache.get(2).get());
        verify(evictionListener).accept(1, "one");
    }

    @Test
    void invalidateAllClearsCacheAndNotifiesForEachEntry() {
        Cache<Integer, String> cache = new CacheLru<>(3, evictionListener);

        cache.put(1, "one");
        cache.put(2, "two");
        cache.put(3, "three");

        cache.invalidateAll();

        assertTrue(cache.get(1).isEmpty());
        assertTrue(cache.get(2).isEmpty());
        assertTrue(cache.get(3).isEmpty());
        verify(evictionListener).accept(1, "one");
        verify(evictionListener).accept(2, "two");
        verify(evictionListener).accept(3, "three");
    }

    @Test
    void constructorRejectsNullEvictionListener() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, () -> new CacheLru<>(2, null));

        assertEquals("Property 'evictedElementConsumer' must not be null.",
                exception.getMessage());
    }

    @Test
    void constructorRejectsLimitTooLow() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new CacheLru<>(0, evictionListener));

        assertEquals("Property 'limit' must be greater than 0",
                exception.getMessage());
        verifyNoInteractions(evictionListener);
    }

    @Test
    void putRejectsNullKey() {
        CacheLru<Integer, String> cache = new CacheLru<>(1, evictionListener);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, () -> cache.put(null, "value"));

        assertEquals("Property 'key' must not be null.",
                exception.getMessage());
        verifyNoInteractions(evictionListener);
    }

    @Test
    void putRejectsNullValue() {
        CacheLru<Integer, String> cache = new CacheLru<>(1, evictionListener);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, () -> cache.put(1, null));

        assertEquals("Property 'value' must not be null.",
                exception.getMessage());
        verifyNoInteractions(evictionListener);
    }

    @Test
    void putNullRejectsNullKey() {
        CacheLru<Integer, String> cache = new CacheLru<>(1, evictionListener);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, () -> cache.putNull(null));

        assertEquals("Property 'key' must not be null.",
                exception.getMessage());
        verifyNoInteractions(evictionListener);
    }

    @Test
    void ivalidateRejectsNullKey() {
        Cache<Integer, String> cache = new CacheLru<>(1, evictionListener);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, () -> cache.ivalidate(null));

        assertEquals("Property 'key' must not be null.",
                exception.getMessage());
        verify(evictionListener, never()).accept(null, null);
    }

    private BiConsumer<Integer, String> failOnEviction() {
        return (key, value) -> fail("Eviction listener should not be invoked");
    }

}
