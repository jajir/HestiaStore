package org.hestiastore.management.restjson;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Simple per-key fixed-window rate limiter.
 */
final class FixedWindowRateLimiter {

    private final Map<String, WindowState> windows = new ConcurrentHashMap<>();
    private final int limitPerMinute;
    private final AtomicLong lastCleanupMinute = new AtomicLong(-1L);
    private final LongSupplier currentMinuteSupplier;

    FixedWindowRateLimiter(final int limitPerMinute) {
        this(limitPerMinute,
                () -> Instant.now().getEpochSecond() / 60L);
    }

    FixedWindowRateLimiter(final int limitPerMinute,
            final LongSupplier currentMinuteSupplier) {
        this.limitPerMinute = limitPerMinute;
        this.currentMinuteSupplier = Objects.requireNonNull(
                currentMinuteSupplier, "currentMinuteSupplier");
    }

    boolean tryAcquire(final String key) {
        final long minute = currentMinuteSupplier.getAsLong();
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
        private volatile long minute;
        private final AtomicInteger counter = new AtomicInteger();

        WindowState(final long minute) {
            this.minute = minute;
        }
    }
}
