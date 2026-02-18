package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

        policy.backoffOrThrow(startNanos, "get", null);
    }
}
