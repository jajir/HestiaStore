package org.hestiastore.index.segmentindex.maintenance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class SegmentIndexMaintenanceImplTest {

    @Test
    void delegatesMaintenanceCommands() {
        final List<String> calls = new ArrayList<>();
        final SegmentIndexMaintenance maintenance =
                new SegmentIndexMaintenanceImpl(
                        () -> calls.add("compact"),
                        () -> calls.add("compactAndWait"),
                        () -> calls.add("flush"),
                        () -> calls.add("flushAndWait"),
                        () -> calls.add("checkAndRepairConsistency"));

        maintenance.compact();
        maintenance.compactAndWait();
        maintenance.flush();
        maintenance.flushAndWait();
        maintenance.checkAndRepairConsistency();

        assertEquals(List.of("compact", "compactAndWait", "flush",
                "flushAndWait", "checkAndRepairConsistency"), calls);
    }

    @Test
    void constructorRejectsMissingActions() {
        final Runnable action = () -> {
        };

        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexMaintenanceImpl(null, action, action,
                        action, action));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexMaintenanceImpl(action, null, action,
                        action, action));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexMaintenanceImpl(action, action, null,
                        action, action));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexMaintenanceImpl(action, action, action,
                        null, action));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentIndexMaintenanceImpl(action, action, action,
                        action, null));
    }
}
