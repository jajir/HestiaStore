package org.hestiastore.index.segmentbridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

class SegmentMaintenanceTaskTest {

    @Test
    void valuesContainExpectedTasks() {
        final List<SegmentMaintenanceTask> values = List
                .of(SegmentMaintenanceTask.values());

        assertEquals(3, values.size());
        assertTrue(values.contains(SegmentMaintenanceTask.FLUSH));
        assertTrue(values.contains(SegmentMaintenanceTask.COMPACT));
        assertTrue(values.contains(SegmentMaintenanceTask.SPLIT));
    }
}
