package org.hestiastore.index.segmentindex.core.split;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SplitMetricsSnapshotTest {

    @Test
    void exposesProvidedCounterValues() {
        final SplitMetricsSnapshot snapshot = new SplitMetricsSnapshot(5, 7);

        assertEquals(5, snapshot.splitInFlightCount());
        assertEquals(7, snapshot.splitBlockedCount());
    }
}
