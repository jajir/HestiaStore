package org.hestiastore.index.segmentindex.core.topology;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class RouteEntryTest {

    @Test
    void acquireAndReleaseLeaseTracksInflightCount() {
        final RouteEntry entry = new RouteEntry(RouteState.ACTIVE);

        entry.acquireLease();

        assertTrue(entry.hasInFlight());
        assertEquals(1L, entry.inFlight());

        entry.releaseLease(SegmentId.of(1));

        assertFalse(entry.hasInFlight());
        assertEquals(0L, entry.inFlight());
    }

    @Test
    void releaseWithoutLeaseFails() {
        final RouteEntry entry = new RouteEntry(RouteState.ACTIVE);

        assertThrows(IllegalStateException.class,
                () -> entry.releaseLease(SegmentId.of(1)));
    }

    @Test
    void stateTransitionsAreExplicit() {
        final RouteEntry entry = new RouteEntry(RouteState.ACTIVE);

        entry.markDraining();
        assertEquals(RouteState.DRAINING, entry.state());

        entry.markActive();
        assertTrue(entry.isActive());

        entry.markRetired();
        assertTrue(entry.isRetired());
    }
}
