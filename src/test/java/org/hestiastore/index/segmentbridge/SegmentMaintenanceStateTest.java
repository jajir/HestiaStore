package org.hestiastore.index.segmentbridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

class SegmentMaintenanceStateTest {

    @Test
    void runExclusive_setsAndClearsActiveTask() {
        final SegmentMaintenanceState state = new SegmentMaintenanceState();
        final AtomicReference<SegmentMaintenanceTask> observed = new AtomicReference<>();

        state.runExclusive(SegmentMaintenanceTask.FLUSH,
                () -> observed.set(state.getActiveTask()));

        assertEquals(SegmentMaintenanceTask.FLUSH, observed.get());
        assertNull(state.getActiveTask());
    }
}
