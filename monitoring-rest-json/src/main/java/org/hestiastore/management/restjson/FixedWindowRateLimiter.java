package org.hestiastore.management.restjson;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple per-key fixed-window rate limiter.
 */
final class FixedWindowRateLimiter {

    private final Map<String, WindowState> windows = new ConcurrentHashMap<>();
    private final int limitPerMinute;
    private final AtomicLong lastCleanupMinute = new AtomicLong(-1L);

    FixedWindowRateLimiter(final int limitPerMinute) {
        this.limitPerMinute = limitPerMinute;
    }

    boolean tryAcquire(final String key) {
        final long minute = Instant.now().getEpochSecond() / 60L;
        cleanupStaleWindows(minute);
        final WindowState state = windows.computeIfAbsent(key,
                k -> new WindowState(minute));
        synchronized (state) {
            if (state.minute != minute) {
                state.minute = minute;
                state.counter.set(0);
            }
            return state.counter.incrementAndGet() <= limitPerMinute;
        }
    }

    private void cleanupStaleWindows(final long minute) {
        if (!lastCleanupMinute.compareAndSet(minute - 1L, minute)) {
            if (lastCleanupMinute.get() >= minute) {
                return;
            }
            lastCleanupMinute.set(minute);
        }
        windows.entrySet().removeIf(e -> e.getValue().minute < minute - 1L);
    }

    private static final class WindowState {
        private long minute;
        private final AtomicInteger counter = new AtomicInteger();

        WindowState(final long minute) {
            this.minute = minute;
        }
    }
}
