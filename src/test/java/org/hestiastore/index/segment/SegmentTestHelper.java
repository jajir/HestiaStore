package org.hestiastore.index.segment;

import java.util.concurrent.TimeUnit;

public final class SegmentTestHelper {

    private static final long CLOSE_TIMEOUT_MILLIS = 5_000L;
    private static final long CLOSE_POLL_MILLIS = 5L;

    private SegmentTestHelper() {
    }

    public static void closeAndAwait(final Segment<?, ?> segment) {
        if (segment == null) {
            return;
        }
        final long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(CLOSE_TIMEOUT_MILLIS);
        while (System.nanoTime() < deadline) {
            final SegmentState state = segment.getState();
            if (state == SegmentState.CLOSED) {
                return;
            }
            if (state == SegmentState.ERROR) {
                throw new AssertionError(String.format(
                        "Segment '%s' closed with ERROR.", segment.getId()));
            }
            final SegmentResult<Void> result = segment.close();
            if (result.getStatus() == SegmentResultStatus.ERROR) {
                throw new AssertionError(String.format(
                        "Segment '%s' failed to close.", segment.getId()));
            }
            try {
                Thread.sleep(CLOSE_POLL_MILLIS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(
                        "Interrupted while waiting for segment close.", e);
            }
        }
        throw new AssertionError(String.format(
                "Timed out waiting for segment '%s' to close.",
                segment.getId()));
    }
}
