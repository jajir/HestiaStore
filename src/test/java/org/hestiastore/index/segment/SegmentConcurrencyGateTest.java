package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class SegmentConcurrencyGateTest {

    @Test
    void freeze_waits_for_in_flight_reads() throws Exception {
        final SegmentConcurrencyGate gate = new SegmentConcurrencyGate();
        assertTrue(gate.tryEnterRead());
        assertEquals(1, gate.getInFlightReads());

        final CountDownLatch started = new CountDownLatch(1);
        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final Thread waiter = new Thread(() -> {
            started.countDown();
            result.complete(gate.tryEnterFreezeAndDrain());
        });
        waiter.start();

        assertTrue(started.await(1, TimeUnit.SECONDS));
        Thread.sleep(10);
        assertFalse(result.isDone());

        gate.exitRead();
        assertEquals(0, gate.getInFlightReads());

        assertTrue(result.get(1, TimeUnit.SECONDS));
        assertEquals(SegmentState.FREEZE, gate.getState());
        assertTrue(gate.finishFreezeToReady());
        waiter.join(1_000);
    }

    @Test
    void tryEnterRead_refuses_freeze() {
        final SegmentConcurrencyGate gate = new SegmentConcurrencyGate();
        assertTrue(gate.tryEnterFreezeAndDrain());

        assertFalse(gate.tryEnterRead());
        assertTrue(gate.finishFreezeToReady());
    }

    @Test
    void tryEnterWrite_allows_maintenance_running() {
        final SegmentConcurrencyGate gate = new SegmentConcurrencyGate();
        assertTrue(gate.tryEnterFreezeAndDrain());
        assertTrue(gate.enterMaintenanceRunning());

        assertTrue(gate.tryEnterWrite());
        assertEquals(1, gate.getInFlightWrites());
        gate.exitWrite();
        assertEquals(0, gate.getInFlightWrites());
    }
}
