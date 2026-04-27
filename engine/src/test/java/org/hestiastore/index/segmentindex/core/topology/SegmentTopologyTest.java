package org.hestiastore.index.segmentindex.core.topology;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.core.split.RouteSplitPlan;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapImpl;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.mapping.Snapshot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentTopologyTest {

    private KeyToSegmentMap<Integer> keyToSegmentMap;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        keyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                new KeyToSegmentMapImpl<>(new MemDirectory(),
                        new TypeDescriptorInteger()));
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
        final SegmentTopology<Integer> topology = newTopology();
        applySplitPlan();
        final long newVersion = keyToSegmentMap.snapshot().version();

        final SegmentTopology.RouteLeaseResult stale = topology.tryAcquire(
                SegmentId.of(1), newVersion);

        assertTrue(stale.isStaleTopology());
    }

    @Test
    void drainWaitsForInflightLeaseAndRejectsNewLease() throws Exception {
        keyToSegmentMap.extendMaxKeyIfNeeded(100);
        final SegmentTopology<Integer> topology = newTopology();
        final SegmentTopology.RouteLease lease = topology
                .tryAcquire(SegmentId.of(0), keyToSegmentMap.snapshot()
                        .version())
                .lease();
        final SegmentTopology.RouteDrain drain = topology
                .tryBeginDrain(SegmentId.of(0)).drain();
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
    void reconcilePublishesChildRoutesAndRetiresParentRoute() {
        keyToSegmentMap.extendMaxKeyIfNeeded(100);
        final SegmentTopology<Integer> topology = newTopology();
        applySplitPlan();

        topology.reconcile(keyToSegmentMap.snapshot());

        final long version = keyToSegmentMap.snapshot().version();
        assertTrue(topology.tryAcquire(SegmentId.of(0), version)
                .isRouteUnavailable());
        try (SegmentTopology.RouteLease lower = topology
                .tryAcquire(SegmentId.of(1), version).lease();
                SegmentTopology.RouteLease upper = topology
                        .tryAcquire(SegmentId.of(2), version).lease()) {
            assertEquals(SegmentId.of(1), lower.segmentId());
            assertEquals(SegmentId.of(2), upper.segmentId());
        }
    }

    @Test
    void reconcileIgnoresOlderSnapshots() {
        keyToSegmentMap.extendMaxKeyIfNeeded(100);
        final Snapshot<Integer> olderSnapshot = keyToSegmentMap.snapshot();
        final SegmentTopology<Integer> topology = SegmentTopology
                .<Integer>builder().snapshot(olderSnapshot).build();
        applySplitPlan();
        final Snapshot<Integer> newerSnapshot = keyToSegmentMap.snapshot();
        topology.reconcile(newerSnapshot);

        topology.reconcile(olderSnapshot);

        assertEquals(newerSnapshot.version(), topology.version());
        assertTrue(topology.tryAcquire(SegmentId.of(0),
                olderSnapshot.version()).isStaleTopology());
        assertTrue(topology.tryAcquire(SegmentId.of(0),
                newerSnapshot.version()).isRouteUnavailable());
        try (SegmentTopology.RouteLease lease = topology
                .tryAcquire(SegmentId.of(1), newerSnapshot.version())
                .lease()) {
            assertEquals(SegmentId.of(1), lease.segmentId());
        }
    }

    private SegmentTopology<Integer> newTopology() {
        return SegmentTopology.<Integer>builder()
                .snapshot(keyToSegmentMap.snapshot()).build();
    }

    private void applySplitPlan() {
        final RouteSplitPlan<Integer> splitPlan = new RouteSplitPlan<>(
                SegmentId.of(0), SegmentId.of(1), SegmentId.of(2), 50,
                RouteSplitPlan.SplitMode.SPLIT);
        assertTrue(keyToSegmentMap.tryApplySplitPlan(splitPlan));
    }
}
