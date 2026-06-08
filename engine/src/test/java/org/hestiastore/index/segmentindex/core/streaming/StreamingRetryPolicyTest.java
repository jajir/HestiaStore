package org.hestiastore.index.segmentindex.core.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class StreamingRetryPolicyTest {

    @Test
    void backoffOrThrow_restoresInterruptStatus() {
        final StreamingRetryPolicy policy = new StreamingRetryPolicy(1, 10);
        Thread.currentThread().interrupt();

        try {
            final IndexException ex = assertThrows(IndexException.class,
                    () -> policy.backoffOrThrow(policy.startNanos(),
                            "openIterator", null));

            assertEquals("Streaming operation 'openIterator' was interrupted",
                    ex.getMessage());
        } finally {
            Thread.interrupted();
        }
    }
}
