package org.hestiastore.index.segmentindex.core.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentindex.routemap.PersistentSegmentRouteMap;
import org.hestiastore.index.segmentindex.routemap.RouteSplitPlan;
import org.hestiastore.index.segmentindex.routemap.RouteMapSnapshot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RouteTopologyTest {

    private SegmentRouteMap<Integer> keyToSegmentMap;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        keyToSegmentMap = new PersistentSegmentRouteMap<>(new MemDirectory(),
                new TypeDescriptorInteger());
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown() {
        if (keyToSegmentMap != null && !keyToSegmentMap.wasClosed()) {
            keyToSegmentMap.close();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void tryAcquireRejectsNewerMapVersionUntilReconciled() {
        keyToSegmentMap.extendMaxKeyIfNeeded(100);
        final RouteTopology<Integer> topology = newTopology();
        applySplitPlan();
        final long newVersion = keyToSegmentMap.snapshot().version();

        final RouteTopology.RouteLeaseResult stale = topology.tryAcquire(
                SegmentId.of(1), newVersion);

        assertTrue(stale.isStaleTopology());
    }

    @Test
    void drainWaitsForInflightLeaseAndRejectsNewLease() throws Exception {
        keyToSegmentMap.extendMaxKeyIfNeeded(100);
        final RouteTopology<Integer> topology = newTopology();
        final RouteTopology.RouteLease lease = topology
                .tryAcquire(SegmentId.of(0), keyToSegmentMap.snapshot()
                        .version())
                .lease();
        final RouteTopology.RouteDrain drain = topology
                .tryBeginDrain(SegmentId.of(0)).orElseThrow();
        final CountDownLatch waiting = new CountDownLatch(1);

        final Future<?> waitFuture = executor.submit(() -> {
            waiting.countDown();
            drain.awaitDrained();
        });

        assertTrue(waiting.await(5, TimeUnit.SECONDS));
        assertFalse(waitFuture.isDone());
        assertTrue(topology.tryAcquire(SegmentId.of(0),
                keyToSegmentMap.snapshot().version()).isRouteUnavailable());

        lease.close();

        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            waitFuture.get(5, TimeUnit.SECONDS);
        });
    }

    @Test
    void drainWaitsForAllInflightLeases() throws Exception {
        keyToSegmentMap.extendMaxKeyIfNeeded(100);
        final RouteTopology<Integer> topology = newTopology();
        final RouteTopology.RouteLease lease = topology
                .tryAcquire(SegmentId.of(0), keyToSegmentMap.snapshot()
                        .version())
                .lease();
        final CountDownLatch waiting = new CountDownLatch(1);

        final Future<?> waitFuture = executor.submit(() -> {
            waiting.countDown();
            topology.drain();
        });

        assertTrue(waiting.await(5, TimeUnit.SECONDS));
        assertFalse(waitFuture.isDone());

        lease.close();

        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            waitFuture.get(5, TimeUnit.SECONDS);
        });
    }

    @Test
    void drainTimesOutWhenInflightLeaseRemainsOpen() {
        keyToSegmentMap.extendMaxKeyIfNeeded(100);
        final RouteTopology<Integer> topology = newTopology(1);
        final RouteTopology.RouteLease lease = topology
                .tryAcquire(SegmentId.of(0), keyToSegmentMap.snapshot()
                        .version())
                .lease();

        try {
            assertThrows(IndexException.class, topology::drain);
        } finally {
            lease.close();
        }
    }

    @Test
    void reconcilePublishesChildRoutesAndRetiresParentRoute() {
        keyToSegmentMap.extendMaxKeyIfNeeded(100);
        final RouteTopology<Integer> topology = newTopology();
        applySplitPlan();

        topology.reconcile(keyToSegmentMap.snapshot());

        final long version = keyToSegmentMap.snapshot().version();
        assertTrue(topology.tryAcquire(SegmentId.of(0), version)
                .isRouteUnavailable());
        try (RouteTopology.RouteLease lower = topology
                .tryAcquire(SegmentId.of(1), version).lease();
                RouteTopology.RouteLease upper = topology
                        .tryAcquire(SegmentId.of(2), version).lease()) {
            assertEquals(SegmentId.of(1), lower.segmentId());
            assertEquals(SegmentId.of(2), upper.segmentId());
        }
    }

    @Test
    void reconcileIgnoresOlderSnapshots() {
        keyToSegmentMap.extendMaxKeyIfNeeded(100);
        final RouteMapSnapshot<Integer> olderSnapshot = keyToSegmentMap.snapshot();
        final RouteTopology<Integer> topology = RouteTopology.create(
                olderSnapshot, 1, 1000);
        applySplitPlan();
        final RouteMapSnapshot<Integer> newerSnapshot = keyToSegmentMap.snapshot();
        topology.reconcile(newerSnapshot);

        topology.reconcile(olderSnapshot);

        assertEquals(newerSnapshot.version(), topology.version());
        assertTrue(topology.tryAcquire(SegmentId.of(0),
                olderSnapshot.version()).isStaleTopology());
        assertTrue(topology.tryAcquire(SegmentId.of(0),
                newerSnapshot.version()).isRouteUnavailable());
        try (RouteTopology.RouteLease lease = topology
                .tryAcquire(SegmentId.of(1), newerSnapshot.version())
                .lease()) {
            assertEquals(SegmentId.of(1), lease.segmentId());
        }
    }

    private RouteTopology<Integer> newTopology() {
        return newTopology(1000);
    }

    private RouteTopology<Integer> newTopology(final int busyTimeoutMillis) {
        return RouteTopology.create(keyToSegmentMap.snapshot(), 1,
                busyTimeoutMillis);
    }

    private void applySplitPlan() {
        final RouteSplitPlan<Integer> split = new RouteSplitPlan<>(
                SegmentId.of(0), SegmentId.of(1), SegmentId.of(2), 50, 100);
        assertTrue(keyToSegmentMap.tryReplaceRouteWithSplit(split));
    }
}
