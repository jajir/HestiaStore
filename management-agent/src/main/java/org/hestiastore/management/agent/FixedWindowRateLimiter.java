package org.hestiastore.management.agent;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple per-key fixed-window rate limiter.
 */
final class FixedWindowRateLimiter {

    private final Map<String, WindowState> windows = new ConcurrentHashMap<>();
    private final int limitPerMinute;

    FixedWindowRateLimiter(final int limitPerMinute) {
        this.limitPerMinute = limitPerMinute;
    }

    boolean tryAcquire(final String key) {
        final long minute = Instant.now().getEpochSecond() / 60L;
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

    private static final class WindowState {
        private long minute;
        private final AtomicInteger counter = new AtomicInteger();

        WindowState(final long minute) {
            this.minute = minute;
        }
    }
}
