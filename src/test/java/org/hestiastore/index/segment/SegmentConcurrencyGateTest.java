package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentConcurrencyGateTest {

    private SegmentConcurrencyGate gate;

    @BeforeEach
    void setUp() {
        gate = new SegmentConcurrencyGate();
    }

    @AfterEach
    void tearDown() {
        gate = null;
    }

    @Test
    void freeze_waits_for_in_flight_operations() throws Exception {
        assertTrue(gate.tryEnterRead());

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

        assertTrue(result.get(1, TimeUnit.SECONDS));
        assertEquals(SegmentState.FREEZE, gate.getState());
        assertTrue(gate.finishFreezeToReady());
        waiter.join(1_000);
    }

    @Test
    void tryEnterRead_refuses_freeze() {
        assertTrue(gate.tryEnterFreezeAndDrain());

        assertFalse(gate.tryEnterRead());
        assertTrue(gate.finishFreezeToReady());
    }

    @Test
    void tryEnterWrite_allows_maintenance_running() {
        assertTrue(gate.tryEnterFreezeAndDrain());
        assertTrue(gate.enterMaintenanceRunning());

        assertTrue(gate.tryEnterWrite());
        gate.exitWrite();
    }

    @Test
    void finishMaintenanceToFreeze_waits_for_in_flight_operations()
            throws Exception {
        assertTrue(gate.tryEnterFreezeAndDrain());
        assertTrue(gate.enterMaintenanceRunning());
        assertTrue(gate.tryEnterRead());

        final CountDownLatch started = new CountDownLatch(1);
        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final Thread waiter = new Thread(() -> {
            started.countDown();
            result.complete(gate.finishMaintenanceToFreeze());
        });
        waiter.start();

        assertTrue(started.await(1, TimeUnit.SECONDS));
        Thread.sleep(10);
        assertFalse(result.isDone());

        gate.exitRead();

        assertTrue(result.get(1, TimeUnit.SECONDS));
        assertEquals(SegmentState.FREEZE, gate.getState());
        assertTrue(gate.finishFreezeToReady());
        waiter.join(1_000);
    }
}
