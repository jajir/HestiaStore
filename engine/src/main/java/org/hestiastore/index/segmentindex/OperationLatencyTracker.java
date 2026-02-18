package org.hestiastore.index.segmentindex;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Lock-free rolling latency tracker used to estimate percentiles.
 */
final class OperationLatencyTracker {

    private static final int SAMPLE_SIZE = 1024;

    private final AtomicLong totalSamples = new AtomicLong();
    private final AtomicLongArray nanos = new AtomicLongArray(SAMPLE_SIZE);

    void recordNanos(final long value) {
        if (value < 0L) {
            return;
        }
        final long cx = totalSamples.getAndIncrement();
        final int index = (int) (cx % SAMPLE_SIZE);
        nanos.set(index, value);
    }

    long percentileMicros(final double percentile) {
        if (percentile <= 0D || percentile > 1D) {
            throw new IllegalArgumentException("percentile must be in (0,1]");
        }
        final long seen = totalSamples.get();
        final int count = (int) Math.min(seen, SAMPLE_SIZE);
        if (count == 0) {
            return 0L;
        }
        final long[] copy = new long[count];
        for (int i = 0; i < count; i++) {
            copy[i] = nanos.get(i);
        }
        Arrays.sort(copy);
        final int index = (int) Math.ceil(percentile * count) - 1;
        final int safeIndex = Math.max(0, Math.min(index, count - 1));
        return copy[safeIndex] / 1000L;
    }
}
