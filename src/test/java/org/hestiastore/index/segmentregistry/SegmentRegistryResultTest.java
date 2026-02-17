package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class SegmentRegistryResultTest {

    @Test
    void okCarriesValue() {
        final SegmentRegistryResult<SegmentId> result = SegmentRegistryResult
                .ok(SegmentId.of(7));

        assertEquals(SegmentRegistryResultStatus.OK, result.getStatus());
        assertEquals(SegmentId.of(7), result.getValue());
        assertTrue(result.isOk());
    }

    @Test
    void busyClosedAndErrorCarryNoValue() {
        final SegmentRegistryResult<Void> busy = SegmentRegistryResult.busy();
        final SegmentRegistryResult<Void> closed = SegmentRegistryResult
                .closed();
        final SegmentRegistryResult<Void> error = SegmentRegistryResult.error();

        assertEquals(SegmentRegistryResultStatus.BUSY, busy.getStatus());
        assertEquals(SegmentRegistryResultStatus.CLOSED, closed.getStatus());
        assertEquals(SegmentRegistryResultStatus.ERROR, error.getStatus());
        assertNull(busy.getValue());
        assertNull(closed.getValue());
        assertNull(error.getValue());
    }

    @Test
    void fromStatusMapsAllStatuses() {
        assertEquals(SegmentRegistryResultStatus.OK,
                SegmentRegistryResult.fromStatus(SegmentRegistryResultStatus.OK)
                        .getStatus());
        assertEquals(SegmentRegistryResultStatus.BUSY, SegmentRegistryResult
                .fromStatus(SegmentRegistryResultStatus.BUSY).getStatus());
        assertEquals(SegmentRegistryResultStatus.CLOSED, SegmentRegistryResult
                .fromStatus(SegmentRegistryResultStatus.CLOSED).getStatus());
        assertEquals(SegmentRegistryResultStatus.ERROR, SegmentRegistryResult
                .fromStatus(SegmentRegistryResultStatus.ERROR).getStatus());
    }
}
