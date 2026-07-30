package org.hestiastore.index.segmentindex.core.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class RouteEntryTest {

    @Test
    void acquireAndReleaseLeaseTracksInflightCount() {
        final RouteEntry entry = new RouteEntry(RouteState.ACTIVE);

        assertTrue(entry.tryAcquireLease());

        assertTrue(entry.hasInFlight());
        assertEquals(1L, entry.inFlight());

        assertFalse(entry.releaseLease(SegmentId.of(1)));

        assertFalse(entry.hasInFlight());
        assertEquals(0L, entry.inFlight());
    }

    @Test
    void releaseWithoutLeaseFails() {
        final RouteEntry entry = new RouteEntry(RouteState.ACTIVE);
        final SegmentId segmentId = SegmentId.of(1);

        assertThrows(IllegalStateException.class,
                () -> entry.releaseLease(segmentId));
    }

    @Test
    void stateTransitionsAreExplicit() {
        final RouteEntry entry = new RouteEntry(RouteState.ACTIVE);

        assertTrue(entry.tryMarkDraining());
        assertEquals(RouteState.DRAINING, entry.state());
        assertFalse(entry.tryAcquireLease());

        entry.markActive();
        assertTrue(entry.isActive());
        assertTrue(entry.tryAcquireLease());
        entry.releaseLease(SegmentId.of(1));

        entry.markRetired();
        assertTrue(entry.isRetired());
        assertFalse(entry.tryAcquireLease());
    }

    @Test
    void abortDrainPreservesInflightLeaseCount() {
        final RouteEntry entry = new RouteEntry(RouteState.ACTIVE);
        assertTrue(entry.tryAcquireLease());

        assertTrue(entry.tryMarkDraining());
        entry.markActive();

        assertTrue(entry.isActive());
        assertEquals(1L, entry.inFlight());
        assertFalse(entry.releaseLease(SegmentId.of(1)));
    }

    @Test
    void awaitDrainedPreservesInterruptStatus() {
        final RouteEntry entry = new RouteEntry(RouteState.ACTIVE);
        final SegmentId segmentId = SegmentId.of(1);
        assertTrue(entry.tryAcquireLease());
        assertTrue(entry.tryMarkDraining());
        Thread.currentThread().interrupt();

        try {
            assertThrows(IndexException.class,
                    () -> entry.awaitDrained(segmentId));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            entry.releaseLease(segmentId);
        }
    }

    @Test
    void concurrentAcquireAndReleaseKeepsCountBalanced() throws Exception {
        final RouteEntry entry = new RouteEntry(RouteState.ACTIVE);
        final ExecutorService executor = Executors.newFixedThreadPool(16);
        final List<Callable<Void>> tasks = new ArrayList<>();
        for (int thread = 0; thread < 16; thread++) {
            tasks.add(() -> {
                for (int iteration = 0; iteration < 10_000; iteration++) {
                    if (!entry.tryAcquireLease()) {
                        throw new IllegalStateException(
                                "Active route rejected a lease.");
                    }
                    entry.releaseLease(SegmentId.of(1));
                }
                return null;
            });
        }

        try {
            final List<Future<Void>> results = executor.invokeAll(tasks);
            for (final Future<Void> result : results) {
                result.get();
            }
            assertEquals(0L, entry.inFlight());
            assertTrue(entry.isActive());
        } finally {
            executor.shutdownNow();
        }
    }
}
