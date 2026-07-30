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
        executor = Executors.newFixedThreadPool(4);
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
        final RouteTopology.RouteLease firstLease = topology
                .tryAcquire(SegmentId.of(0), keyToSegmentMap.snapshot()
                        .version())
                .lease();
        final RouteTopology.RouteLease secondLease = topology
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

        firstLease.close();
        assertFalse(waitFuture.isDone());
        secondLease.close();

        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            waitFuture.get(5, TimeUnit.SECONDS);
        });
    }

    @Test
    void acquireAndDrainRaceLeavesRouteUnavailableAfterDrainWins()
            throws Exception {
        keyToSegmentMap.extendMaxKeyIfNeeded(100);
        final RouteTopology<Integer> topology = newTopology();
        final SegmentId segmentId = SegmentId.of(0);
        final long version = keyToSegmentMap.snapshot().version();
        final CountDownLatch ready = new CountDownLatch(2);
        final CountDownLatch start = new CountDownLatch(1);

        final Future<RouteTopology.RouteLeaseResult> acquireFuture = executor
                .submit(() -> {
                    ready.countDown();
                    start.await();
                    return topology.tryAcquire(segmentId, version);
                });
        final Future<RouteTopology.RouteDrain> drainFuture = executor
                .submit(() -> {
                    ready.countDown();
                    start.await();
                    return topology.tryBeginDrain(segmentId).orElseThrow();
                });

        assertTrue(ready.await(5, TimeUnit.SECONDS));
        start.countDown();
        final RouteTopology.RouteLeaseResult acquire = acquireFuture.get(5,
                TimeUnit.SECONDS);
        final RouteTopology.RouteDrain drain = drainFuture.get(5,
                TimeUnit.SECONDS);

        assertTrue(topology.tryAcquire(segmentId, version)
                .isRouteUnavailable());
        if (acquire.isAcquired()) {
            acquire.lease().close();
        } else {
            assertTrue(acquire.isRouteUnavailable());
        }
        assertTimeoutPreemptively(Duration.ofSeconds(5), drain::awaitDrained);
        drain.abort();
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
    void routeDrainDoesNotBlockAnotherRoute() {
        keyToSegmentMap.extendMaxKeyIfNeeded(100);
        final RouteTopology<Integer> topology = newTopology();
        applySplitPlan();
        topology.reconcile(keyToSegmentMap.snapshot());
        final long version = keyToSegmentMap.snapshot().version();
        final RouteTopology.RouteLease lower = topology
                .tryAcquire(SegmentId.of(1), version).lease();
        final RouteTopology.RouteDrain drain = topology
                .tryBeginDrain(SegmentId.of(1)).orElseThrow();

        try (RouteTopology.RouteLease upper = topology
                .tryAcquire(SegmentId.of(2), version).lease()) {
            assertEquals(SegmentId.of(2), upper.segmentId());
            assertTrue(topology.tryAcquire(SegmentId.of(1), version)
                    .isRouteUnavailable());
        } finally {
            lower.close();
        }

        assertTimeoutPreemptively(Duration.ofSeconds(5), drain::awaitDrained);
        drain.abort();
    }

    @Test
    void reconcileRetainsRetiredLeaseForGlobalDrain() throws Exception {
        keyToSegmentMap.extendMaxKeyIfNeeded(100);
        final RouteTopology<Integer> topology = newTopology();
        final RouteTopology.RouteLease parent = topology
                .tryAcquire(SegmentId.of(0), keyToSegmentMap.snapshot()
                        .version())
                .lease();
        applySplitPlan();
        topology.reconcile(keyToSegmentMap.snapshot());
        final CountDownLatch waiting = new CountDownLatch(1);

        final Future<?> waitFuture = executor.submit(() -> {
            waiting.countDown();
            topology.drain();
        });

        assertTrue(waiting.await(5, TimeUnit.SECONDS));
        assertFalse(waitFuture.isDone());
        parent.close();
        assertTimeoutPreemptively(Duration.ofSeconds(5),
                () -> waitFuture.get(5, TimeUnit.SECONDS));
    }

    @Test
    void abortDrainAllowsAnotherDrainCycle() {
        keyToSegmentMap.extendMaxKeyIfNeeded(100);
        final RouteTopology<Integer> topology = newTopology();
        final SegmentId segmentId = SegmentId.of(0);

        topology.tryBeginDrain(segmentId).orElseThrow().abort();

        try (RouteTopology.RouteLease lease = topology
                .tryAcquire(segmentId, keyToSegmentMap.snapshot().version())
                .lease()) {
            assertEquals(segmentId, lease.segmentId());
        }
        final RouteTopology.RouteDrain secondDrain = topology
                .tryBeginDrain(segmentId).orElseThrow();
        secondDrain.awaitDrained();
        secondDrain.abort();
        try (RouteTopology.RouteLease lease = topology.tryAcquire(segmentId,
                keyToSegmentMap.snapshot().version()).lease()) {
            assertEquals(segmentId, lease.segmentId());
        }
    }

    @Test
    void closingLeaseConcurrentlyIsIdempotent() throws Exception {
        keyToSegmentMap.extendMaxKeyIfNeeded(100);
        final RouteTopology<Integer> topology = newTopology();
        final long version = keyToSegmentMap.snapshot().version();
        final RouteTopology.RouteLease lease = topology
                .tryAcquire(SegmentId.of(0), version).lease();

        final Future<?> firstClose = executor.submit(lease::close);
        final Future<?> secondClose = executor.submit(lease::close);

        firstClose.get(5, TimeUnit.SECONDS);
        secondClose.get(5, TimeUnit.SECONDS);
        try (RouteTopology.RouteLease nextLease = topology
                .tryAcquire(SegmentId.of(0), version).lease()) {
            assertEquals(SegmentId.of(0), nextLease.segmentId());
        }
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

    @Test
    void concurrentReconcileKeepsNewestTopology() throws Exception {
        keyToSegmentMap.extendMaxKeyIfNeeded(100);
        final RouteMapSnapshot<Integer> olderSnapshot = keyToSegmentMap.snapshot();
        final RouteTopology<Integer> topology = RouteTopology.create(
                olderSnapshot, 1, 1000);
        applySplitPlan();
        final RouteMapSnapshot<Integer> newerSnapshot = keyToSegmentMap.snapshot();
        final CountDownLatch ready = new CountDownLatch(2);
        final CountDownLatch start = new CountDownLatch(1);

        final Future<?> olderReconcile = executor.submit(() -> {
            ready.countDown();
            start.await();
            topology.reconcile(olderSnapshot);
            return null;
        });
        final Future<?> newerReconcile = executor.submit(() -> {
            ready.countDown();
            start.await();
            topology.reconcile(newerSnapshot);
            return null;
        });

        assertTrue(ready.await(5, TimeUnit.SECONDS));
        start.countDown();
        olderReconcile.get(5, TimeUnit.SECONDS);
        newerReconcile.get(5, TimeUnit.SECONDS);

        assertEquals(newerSnapshot.version(), topology.version());
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
