package org.hestiastore.management.restjson;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

class FixedWindowRateLimiterTest {

    @Test
    void tryAcquireRejectsRequestsAfterLimitReachedWithinSameMinute() {
        final FixedWindowRateLimiter limiter = new FixedWindowRateLimiter(2,
                () -> 10L);

        assertTrue(limiter.tryAcquire("node-a"));
        assertTrue(limiter.tryAcquire("node-a"));
        assertFalse(limiter.tryAcquire("node-a"));
    }

    @Test
    void tryAcquireResetsCounterWhenMinuteAdvances() {
        final AtomicLong minute = new AtomicLong(10L);
        final FixedWindowRateLimiter limiter = new FixedWindowRateLimiter(1,
                minute::get);

        assertTrue(limiter.tryAcquire("node-a"));
        assertFalse(limiter.tryAcquire("node-a"));

        minute.incrementAndGet();

        assertTrue(limiter.tryAcquire("node-a"));
    }

    @Test
    void cleanupRemovesWindowsOlderThanPreviousMinute() {
        final AtomicLong minute = new AtomicLong(10L);
        final FixedWindowRateLimiter limiter = new FixedWindowRateLimiter(1,
                minute::get);

        assertTrue(limiter.tryAcquire("stale"));
        minute.set(11L);
        assertTrue(limiter.tryAcquire("recent"));
        minute.set(12L);
        assertTrue(limiter.tryAcquire("current"));

        final Map<String, ?> windows = readWindows(limiter);
        assertEquals(2, windows.size());
        assertFalse(windows.containsKey("stale"));
        assertTrue(windows.containsKey("recent"));
        assertTrue(windows.containsKey("current"));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, ?> readWindows(
            final FixedWindowRateLimiter limiter) {
        try {
            final Field windowsField = FixedWindowRateLimiter.class
                    .getDeclaredField("windows");
            windowsField.setAccessible(true);
            return (Map<String, ?>) windowsField.get(limiter);
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException("Unable to read limiter windows",
                    ex);
        }
    }
}
