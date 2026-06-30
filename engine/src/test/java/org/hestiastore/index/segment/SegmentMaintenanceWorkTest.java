package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentMaintenanceWorkTest {

    private Runnable ioWork;
    private Runnable publishWork;
    private SegmentMaintenanceWork work;

    @BeforeEach
    void setUp() {
        ioWork = () -> {
        };
        publishWork = () -> {
        };
        work = new SegmentMaintenanceWork(ioWork, publishWork);
    }

    @AfterEach
    void tearDown() {
        work = null;
        ioWork = null;
        publishWork = null;
    }

    @Test
    void constructorRejectsNullIoWork() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentMaintenanceWork(null, publishWork));
        assertEquals("Property 'ioWork' must not be null.", ex.getMessage());
    }

    @Test
    void constructorRejectsNullPublishWork() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentMaintenanceWork(ioWork, null));
        assertEquals("Property 'publishWork' must not be null.",
                ex.getMessage());
    }

    @Test
    void accessorsReturnSuppliedRunnables() {
        assertSame(ioWork, work.ioWork());
        assertSame(publishWork, work.publishWork());
    }
}
