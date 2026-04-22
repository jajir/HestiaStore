package org.hestiastore.index.segmentregistry;

import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.OperationResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class SegmentRegistryResultTest {

    @Test
    void okCarriesValue() {
        final OperationResult<SegmentId> result = SegmentRegistryResult
                .ok(SegmentId.of(7));

        assertEquals(OperationStatus.OK, result.getStatus());
        assertEquals(SegmentId.of(7), result.getValue());
        assertTrue(result.isOk());
    }

    @Test
    void busyClosedAndErrorCarryNoValue() {
        final OperationResult<Void> busy = OperationResult.busy();
        final OperationResult<Void> closed = SegmentRegistryResult
                .closed();
        final OperationResult<Void> error = OperationResult.error();

        assertEquals(OperationStatus.BUSY, busy.getStatus());
        assertEquals(OperationStatus.CLOSED, closed.getStatus());
        assertEquals(OperationStatus.ERROR, error.getStatus());
        assertNull(busy.getValue());
        assertNull(closed.getValue());
        assertNull(error.getValue());
    }

    @Test
    void fromStatusMapsAllStatuses() {
        assertEquals(OperationStatus.OK,
                OperationResult.fromStatus(OperationStatus.OK)
                        .getStatus());
        assertEquals(OperationStatus.BUSY, SegmentRegistryResult
                .fromStatus(OperationStatus.BUSY).getStatus());
        assertEquals(OperationStatus.CLOSED, SegmentRegistryResult
                .fromStatus(OperationStatus.CLOSED).getStatus());
        assertEquals(OperationStatus.ERROR, SegmentRegistryResult
                .fromStatus(OperationStatus.ERROR).getStatus());
    }
}
