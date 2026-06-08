package org.hestiastore.index.segmentindex.core.segmentlease;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.Test;

class SegmentAccessRetryPolicyTest {

    @Test
    void backoffOrThrow_allowsRetryBeforeTimeout() {
        final SegmentAccessRetryPolicy policy =
                new SegmentAccessRetryPolicy(1, 10);

        assertDoesNotThrow(() -> policy.backoffOrThrow(policy.startNanos(),
                "acquireForWrite", "segment-00000"));
    }
}
