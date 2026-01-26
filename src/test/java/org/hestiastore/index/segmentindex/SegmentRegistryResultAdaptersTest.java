package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.junit.jupiter.api.Test;

class SegmentRegistryResultAdaptersTest {

    @Test
    void fromSegmentResultMapsOkValue() {
        final SegmentResult<String> input = SegmentResult.ok("value");

        final SegmentRegistryResult<String> result = SegmentRegistryResultAdapters
                .fromSegmentResult(input);

        assertEquals(SegmentRegistryResultStatus.OK, result.getStatus());
        assertEquals("value", result.getValue());
    }

    @Test
    void fromSegmentResultMapsBusy() {
        final SegmentRegistryResult<String> result = SegmentRegistryResultAdapters
                .fromSegmentResult(SegmentResult.busy());

        assertEquals(SegmentRegistryResultStatus.BUSY, result.getStatus());
        assertNull(result.getValue());
    }

    @Test
    void toSegmentResultMapsOkValue() {
        final SegmentRegistryResult<String> input = SegmentRegistryResult
                .ok("value");

        final SegmentResult<String> result = SegmentRegistryResultAdapters
                .toSegmentResult(input);

        assertEquals(SegmentResultStatus.OK, result.getStatus());
        assertEquals("value", result.getValue());
    }

    @Test
    void toSegmentResultMapsClosed() {
        final SegmentResult<String> result = SegmentRegistryResultAdapters
                .toSegmentResult(SegmentRegistryResult.closed());

        assertEquals(SegmentResultStatus.CLOSED, result.getStatus());
        assertNull(result.getValue());
    }

    @Test
    void statusAdaptersMapBothWays() {
        assertEquals(SegmentRegistryResultStatus.ERROR,
                SegmentRegistryResultAdapters
                        .fromSegmentStatus(SegmentResultStatus.ERROR));
        assertEquals(SegmentResultStatus.BUSY,
                SegmentRegistryResultAdapters
                        .toSegmentStatus(SegmentRegistryResultStatus.BUSY));
    }
}
