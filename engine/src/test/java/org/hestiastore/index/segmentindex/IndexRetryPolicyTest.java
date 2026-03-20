package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class IndexRetryPolicyTest {

    @Test
    void backoffOrThrow_throwsWhenTimedOut() {
        final IndexRetryPolicy policy = new IndexRetryPolicy(1, 1);
        final long startNanos = System.nanoTime()
                - TimeUnit.MILLISECONDS.toNanos(5);
        final SegmentId segmentId = SegmentId.of(1);

        final IndexException ex = assertThrows(IndexException.class,
                () -> policy.backoffOrThrow(startNanos, "get", segmentId));

        assertEquals(
                "Index operation 'get' timed out after 1 ms on segment '"
                        + segmentId + "'",
                ex.getMessage());
    }

    @Test
    void backoffOrThrow_doesNotThrowBeforeTimeout() {
        final IndexRetryPolicy policy = new IndexRetryPolicy(1, 10);
        final long startNanos = policy.startNanos();

        assertDoesNotThrow(() -> policy.backoffOrThrow(startNanos, "get",
                null));
    }

    @Test
    void backoffOrThrow_preservesInterruptStatus() {
        final IndexRetryPolicy policy = new IndexRetryPolicy(1, 10);
        final long startNanos = policy.startNanos();
        Thread.currentThread().interrupt();
        try {
            final IndexException ex = assertThrows(IndexException.class,
                    () -> policy.backoffOrThrow(startNanos, "get", null));

            assertEquals("Index operation 'get' was interrupted",
                    ex.getMessage());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }
}
