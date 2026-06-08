package org.hestiastore.index.segmentindex.core.maintenance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.TimeUnit;

import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class MaintenanceRetryPolicyTest {

    @Test
    void backoffOrThrow_formatsMaintenanceTimeout() {
        final MaintenanceRetryPolicy policy = new MaintenanceRetryPolicy(1, 1);
        final long startNanos = System.nanoTime()
                - TimeUnit.MILLISECONDS.toNanos(5);

        final IndexException ex = assertThrows(IndexException.class,
                () -> policy.backoffOrThrow(startNanos, "flush", "segment"));

        assertEquals(
                "Maintenance operation 'flush' timed out after 1 ms on target 'segment'",
                ex.getMessage());
    }
}
