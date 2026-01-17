package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

class SegmentMaintenanceServiceTest {

    @Test
    void startMaintenance_invokesOnReadyAfterReady() {
        final SegmentConcurrencyGate gate = new SegmentConcurrencyGate();
        final SegmentMaintenanceService service = new SegmentMaintenanceService(
                gate, new DirectExecutor());
        final AtomicReference<SegmentState> stateAtCallback = new AtomicReference<>();
        final AtomicBoolean called = new AtomicBoolean(false);

        final SegmentResult<java.util.concurrent.CompletionStage<Void>> result = service
                .startMaintenance(() -> new SegmentMaintenanceWork(() -> {
                }, () -> {
                }), () -> {
                    stateAtCallback.set(gate.getState());
                    called.set(true);
                });

        assertEquals(SegmentResultStatus.OK, result.getStatus());
        result.getValue().toCompletableFuture().join();

        assertTrue(called.get());
        assertEquals(SegmentState.READY, stateAtCallback.get());
        assertEquals(SegmentState.READY, gate.getState());
    }

    @Test
    void startMaintenance_doesNotInvokeOnReadyWhenPublishFails() {
        final SegmentConcurrencyGate gate = new SegmentConcurrencyGate();
        final SegmentMaintenanceService service = new SegmentMaintenanceService(
                gate, new DirectExecutor());
        final AtomicBoolean called = new AtomicBoolean(false);

        final SegmentResult<java.util.concurrent.CompletionStage<Void>> result = service
                .startMaintenance(() -> new SegmentMaintenanceWork(() -> {
                }, () -> {
                    throw new IllegalStateException("boom");
                }), () -> called.set(true));

        assertEquals(SegmentResultStatus.OK, result.getStatus());
        assertThrows(CompletionException.class,
                () -> result.getValue().toCompletableFuture().join());

        assertFalse(called.get());
        assertEquals(SegmentState.ERROR, gate.getState());
    }

    @Test
    void startMaintenance_propagatesOnReadyFailure() {
        final SegmentConcurrencyGate gate = new SegmentConcurrencyGate();
        final SegmentMaintenanceService service = new SegmentMaintenanceService(
                gate, new DirectExecutor());

        final SegmentResult<java.util.concurrent.CompletionStage<Void>> result = service
                .startMaintenance(() -> new SegmentMaintenanceWork(() -> {
                }, () -> {
                }), () -> {
                    throw new IllegalStateException("on ready fail");
                });

        assertEquals(SegmentResultStatus.OK, result.getStatus());
        assertThrows(CompletionException.class,
                () -> result.getValue().toCompletableFuture().join());
        assertEquals(SegmentState.READY, gate.getState());
    }
}
