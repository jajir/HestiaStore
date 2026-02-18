package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SegmentSplitAbortExceptionTest {

    @Test
    void storesProvidedMessage() {
        final SegmentSplitAbortException exception = new SegmentSplitAbortException(
                "abort");

        assertEquals("abort", exception.getMessage());
    }
}
