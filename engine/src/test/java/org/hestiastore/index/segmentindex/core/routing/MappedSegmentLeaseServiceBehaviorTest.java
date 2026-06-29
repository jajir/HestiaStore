package org.hestiastore.index.segmentindex.core.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.SegmentWindow;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentindex.routemap.PersistentSegmentRouteMap;
import org.hestiastore.index.segmentindex.routemap.RouteSplitPlan;
import org.hestiastore.index.segmentindex.routemap.RouteMapSnapshot;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MappedSegmentLeaseServiceBehaviorTest {

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private SegmentRegistry.Runtime<Integer, String> registryRuntime;

    @Mock
    private BlockingSegment<Integer, String> blockingSegment;

    @Mock
    private BusyRetryPolicy retryPolicy;

    private SegmentRouteMap<Integer> keyToSegmentMap;
    private RouteTopology<Integer> segmentTopology;
    private MappedSegmentLeaseService<Integer, String> service;

    @BeforeEach
    void setUp() {
        keyToSegmentMap = new PersistentSegmentRouteMap<>(new MemDirectory(),
                new TypeDescriptorInteger());
        segmentTopology = RouteTopology.create(keyToSegmentMap.snapshot(),
                1, 1000);
        service = new MappedSegmentLeaseService<>(keyToSegmentMap,
                segmentRegistry, segmentTopology, retryPolicy);
    }

    @AfterEach
    void tearDown() {
        if (keyToSegmentMap != null && !keyToSegmentMap.wasClosed()) {
            keyToSegmentMap.close();
        }
    }

    @Test
    void acquireForReadReturnsNullWhenKeyHasNoRoute() {
        final MappedSegmentLease<Integer, String> result = service
                .acquireForRead(10);

        assertNull(result);
        verifyNoInteractions(segmentRegistry);
    }

    @Test
    void acquireForWriteCreatesBootstrapRouteAndReturnsLease() {
        when(segmentRegistry.loadSegment(SegmentId.of(0)))
                .thenReturn(blockingSegment);

        try (MappedSegmentLease<Integer, String> lease = service
                .acquireForWrite(11)) {
            assertSame(blockingSegment, lease.segment());
            assertEquals(SegmentId.of(0), lease.segmentId());
        }

        assertEquals(SegmentId.of(0),
                keyToSegmentMap.findSegmentIdForKey(11));
        verify(segmentRegistry).loadSegment(SegmentId.of(0));
    }

    @Test
    void acquireForWriteUsesOpenTailWithoutExtendingRouteMap() {
        keyToSegmentMap.extendMaxKeyIfNeeded(10);
        segmentTopology.reconcile(keyToSegmentMap.snapshot());
        final RouteMapSnapshot<Integer> before = keyToSegmentMap.snapshot();
        when(segmentRegistry.loadSegment(SegmentId.of(0)))
                .thenReturn(blockingSegment);

        try (MappedSegmentLease<Integer, String> lease = service
                .acquireForWrite(20)) {
            assertSame(blockingSegment, lease.segment());
            assertEquals(SegmentId.of(0), lease.segmentId());
        }

        assertTrue(keyToSegmentMap.isAtVersion(before.version()));
        assertEquals(SegmentId.of(0),
                keyToSegmentMap.findSegmentIdForKey(20));
        verify(retryPolicy, never()).backoffOrThrow(0L, "acquireForWrite",
                SegmentId.of(0));
    }

    @Test
    void acquireForWriteProvidesSegmentForCallerOperation() {
        when(segmentRegistry.loadSegment(SegmentId.of(0)))
                .thenReturn(blockingSegment);

        try (MappedSegmentLease<Integer, String> lease = service
                .acquireForWrite(11)) {
            lease.segment().put(11, "v11");
        }

        verify(blockingSegment).put(11, "v11");
    }

    @Test
    void drainDelegatesToTopology() {
        final RouteTopology<Integer> topology = mockSegmentTopology();
        final MappedSegmentLeaseService<Integer, String> leaseService = new MappedSegmentLeaseService<>(keyToSegmentMap,
                segmentRegistry,
                topology, retryPolicy);

        leaseService.drain();

        verify(topology).drain();
    }

    @Test
    void getSegmentIdsReturnsRoutedWindowIds() {
        keyToSegmentMap.extendMaxKeyIfNeeded(10);

        final List<SegmentId> result = service.getSegmentIds(
                SegmentWindow.unbounded());

        assertEquals(List.of(SegmentId.of(0)), result);
    }

    @Test
    void snapshotSegmentIdsTracksRouteVersion() {
        keyToSegmentMap.extendMaxKeyIfNeeded(10);

        final RouteWindowSnapshot snapshot = service.snapshotSegmentIds(
                SegmentWindow.unbounded());

        assertEquals(List.of(SegmentId.of(0)), snapshot.segmentIds());
        assertTrue(service.isCurrent(snapshot));

        final boolean replaced = keyToSegmentMap.tryReplaceRouteWithSplit(
                new RouteSplitPlan<>(SegmentId.of(0), SegmentId.of(1),
                        SegmentId.of(2), 5, 10));

        assertTrue(replaced);
        assertFalse(service.isCurrent(snapshot));
    }

    @Test
    void acquireMappedSegmentReturnsLoadedLeaseAndClosesRouteLease() {
        keyToSegmentMap.extendMaxKeyIfNeeded(10);
        final RouteMapSnapshot<Integer> snapshot = keyToSegmentMap.snapshot();
        final RouteTopology<Integer> topology = mockTopologyWithRouteLease(
                snapshot);
        final RouteTopology.RouteLease lease = topology
                .tryAcquire(SegmentId.of(0), snapshot.version()).lease();
        when(lease.segmentId()).thenReturn(SegmentId.of(0));
        final MappedSegmentLeaseService<Integer, String> leaseService = new MappedSegmentLeaseService<>(keyToSegmentMap,
                segmentRegistry, topology, retryPolicy);
        when(segmentRegistry.loadSegment(SegmentId.of(0)))
                .thenReturn(blockingSegment);

        try (MappedSegmentLease<Integer, String> acquired = leaseService
                .acquireMappedSegment(SegmentId.of(0))) {
            assertSame(blockingSegment, acquired.segment());
            assertEquals(SegmentId.of(0), acquired.segmentId());
        }

        verify(lease).close();
    }

    @Test
    void acquireForWriteClosesRouteLeaseWhenSegmentLoadFails() {
        keyToSegmentMap.extendMaxKeyIfNeeded(10);
        final RouteMapSnapshot<Integer> snapshot = keyToSegmentMap.snapshot();
        final RouteTopology<Integer> topology = mockSegmentTopology();
        final RouteTopology.RouteLeaseResult result = mock(
                RouteTopology.RouteLeaseResult.class);
        final RouteTopology.RouteLease lease = mock(
                RouteTopology.RouteLease.class);
        when(topology.tryAcquire(SegmentId.of(0), snapshot.version()))
                .thenReturn(result);
        when(result.isAcquired()).thenReturn(true);
        when(result.lease()).thenReturn(lease);
        when(lease.segmentId()).thenReturn(SegmentId.of(0));
        final MappedSegmentLeaseService<Integer, String> leaseService = new MappedSegmentLeaseService<>(keyToSegmentMap,
                segmentRegistry, topology, retryPolicy);
        final IllegalStateException failure = new IllegalStateException(
                "failed");
        when(segmentRegistry.loadSegment(SegmentId.of(0))).thenThrow(failure);

        assertThrows(IllegalStateException.class,
                () -> leaseService.acquireForWrite(10));

        verify(lease).close();
    }

    @Test
    void acquireForReadReconcilesStaleTopologyAndRetries() {
        keyToSegmentMap.extendMaxKeyIfNeeded(10);
        final RouteMapSnapshot<Integer> snapshot = keyToSegmentMap.snapshot();
        final RouteTopology<Integer> topology = mockTopologyStaleThenAcquired(
                snapshot);
        final MappedSegmentLeaseService<Integer, String> leaseService = new MappedSegmentLeaseService<>(keyToSegmentMap,
                segmentRegistry, topology, retryPolicy);
        when(segmentRegistry.loadSegment(SegmentId.of(0)))
                .thenReturn(blockingSegment);

        try (MappedSegmentLease<Integer, String> lease = leaseService
                .acquireForRead(10)) {
            assertSame(blockingSegment, lease.segment());
        }

        verify(topology).reconcile(any());
    }

    @Test
    void tryAcquireMappedSegmentReturnsLeaseAndClosesRouteLease() {
        keyToSegmentMap.extendMaxKeyIfNeeded(10);
        final RouteMapSnapshot<Integer> snapshot = keyToSegmentMap.snapshot();
        final RouteTopology<Integer> topology = mockTopologyWithRouteLease(
                snapshot);
        final RouteTopology.RouteLease lease = topology
                .tryAcquire(SegmentId.of(0), snapshot.version()).lease();
        when(lease.segmentId()).thenReturn(SegmentId.of(0));
        final MappedSegmentLeaseService<Integer, String> leaseService = new MappedSegmentLeaseService<>(keyToSegmentMap,
                segmentRegistry, topology, retryPolicy);
        when(segmentRegistry.tryGetSegment(SegmentId.of(0)))
                .thenReturn(Optional.of(blockingSegment));

        final Optional<MappedSegmentLease<Integer, String>> result = leaseService
                .tryAcquireMappedSegment(SegmentId.of(0));

        assertTrue(result.isPresent());
        try (MappedSegmentLease<Integer, String> acquired = result.get()) {
            assertSame(blockingSegment, acquired.segment());
            assertEquals(SegmentId.of(0), acquired.segmentId());
        }
        verify(lease).close();
    }

    @Test
    void tryAcquireMappedSegmentReturnsEmptyAndClosesRouteLeaseWhenUnavailable() {
        keyToSegmentMap.extendMaxKeyIfNeeded(10);
        final RouteMapSnapshot<Integer> snapshot = keyToSegmentMap.snapshot();
        final RouteTopology<Integer> topology = mockTopologyWithRouteLease(
                snapshot);
        final RouteTopology.RouteLease lease = topology
                .tryAcquire(SegmentId.of(0), snapshot.version()).lease();
        final MappedSegmentLeaseService<Integer, String> leaseService = new MappedSegmentLeaseService<>(keyToSegmentMap,
                segmentRegistry, topology, retryPolicy);
        when(segmentRegistry.tryGetSegment(SegmentId.of(0)))
                .thenReturn(Optional.empty());

        final Optional<MappedSegmentLease<Integer, String>> result = leaseService
                .tryAcquireMappedSegment(SegmentId.of(0));

        assertTrue(result.isEmpty());
        verify(lease).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    void getLoadedMappedSegmentIdsReturnsOnlyLoadedMappedSegments() {
        keyToSegmentMap.extendMaxKeyIfNeeded(10);
        final BlockingSegment<Integer, String> unmappedSegment = mock(
                BlockingSegment.class);
        when(segmentRegistry.runtime()).thenReturn(registryRuntime);
        when(registryRuntime.loadedSegmentsSnapshot()).thenReturn(
                List.of(blockingSegment, unmappedSegment));
        when(blockingSegment.getId()).thenReturn(SegmentId.of(0));
        when(unmappedSegment.getId()).thenReturn(SegmentId.of(9));

        final List<SegmentId> result = service.getLoadedMappedSegmentIds();

        assertEquals(List.of(SegmentId.of(0)), result);
    }

    @Test
    void getLoadedMappedSegmentIdsReturnsEmptyWithoutRegistryLookupWhenNoRoutes() {
        final List<SegmentId> result = service.getLoadedMappedSegmentIds();

        assertTrue(result.isEmpty());
        verify(segmentRegistry, never()).runtime();
    }

    @Test
    void tryAcquireLoadedMappedSegmentReturnsLeaseAndClosesRouteLease() {
        keyToSegmentMap.extendMaxKeyIfNeeded(10);
        final RouteMapSnapshot<Integer> snapshot = keyToSegmentMap.snapshot();
        final RouteTopology<Integer> topology = mockTopologyWithRouteLease(
                snapshot);
        final RouteTopology.RouteLease lease = topology
                .tryAcquire(SegmentId.of(0), snapshot.version()).lease();
        when(lease.segmentId()).thenReturn(SegmentId.of(0));
        final MappedSegmentLeaseService<Integer, String> leaseService = new MappedSegmentLeaseService<>(keyToSegmentMap,
                segmentRegistry, topology, retryPolicy);
        when(segmentRegistry.tryGetLoadedSegment(SegmentId.of(0)))
                .thenReturn(Optional.of(blockingSegment));

        final Optional<MappedSegmentLease<Integer, String>> result = leaseService
                .tryAcquireLoadedMappedSegment(SegmentId.of(0));

        assertTrue(result.isPresent());
        try (MappedSegmentLease<Integer, String> acquired = result.get()) {
            assertSame(blockingSegment, acquired.segment());
            assertEquals(SegmentId.of(0), acquired.segmentId());
        }
        verify(segmentRegistry, never()).tryGetSegment(SegmentId.of(0));
        verify(lease).close();
    }

    @Test
    void tryAcquireLoadedMappedSegmentReturnsEmptyAndClosesRouteLeaseWhenUnloaded() {
        keyToSegmentMap.extendMaxKeyIfNeeded(10);
        final RouteMapSnapshot<Integer> snapshot = keyToSegmentMap.snapshot();
        final RouteTopology<Integer> topology = mockTopologyWithRouteLease(
                snapshot);
        final RouteTopology.RouteLease lease = topology
                .tryAcquire(SegmentId.of(0), snapshot.version()).lease();
        final MappedSegmentLeaseService<Integer, String> leaseService = new MappedSegmentLeaseService<>(keyToSegmentMap,
                segmentRegistry, topology, retryPolicy);
        when(segmentRegistry.tryGetLoadedSegment(SegmentId.of(0)))
                .thenReturn(Optional.empty());

        final Optional<MappedSegmentLease<Integer, String>> result = leaseService
                .tryAcquireLoadedMappedSegment(SegmentId.of(0));

        assertTrue(result.isEmpty());
        verify(segmentRegistry, never()).tryGetSegment(SegmentId.of(0));
        verify(lease).close();
    }

    @Test
    void tryAcquireForSplitReturnsEmptyWhenDrainUnavailable() {
        final RouteTopology<Integer> topology = mockSegmentTopology();
        when(topology.tryBeginDrain(SegmentId.of(0))).thenReturn(
                Optional.empty());
        final MappedSegmentLeaseService<Integer, String> leaseService = new MappedSegmentLeaseService<>(keyToSegmentMap,
                segmentRegistry, topology, retryPolicy);

        final Optional<RouteSplitLease<Integer, String>> result = leaseService.tryAcquireForSplit(SegmentId.of(0));

        assertTrue(result.isEmpty());
        verifyNoInteractions(segmentRegistry);
    }

    @Test
    void splitLeaseAbortCallsDrainAbort() {
        keyToSegmentMap.extendMaxKeyIfNeeded(10);
        final RouteTopology<Integer> topology = mockSegmentTopology();
        final RouteTopology.RouteDrain drain = mockRouteDrain(topology);
        final MappedSegmentLeaseService<Integer, String> leaseService = new MappedSegmentLeaseService<>(keyToSegmentMap,
                segmentRegistry, topology, retryPolicy);
        when(segmentRegistry.tryGetSegment(SegmentId.of(0)))
                .thenReturn(Optional.of(blockingSegment));

        final Optional<RouteSplitLease<Integer, String>> result = leaseService.tryAcquireForSplit(SegmentId.of(0));

        assertTrue(result.isPresent());
        result.get().abort();
        verify(drain).abort();
        verify(drain, never()).complete();
    }

    @Test
    void splitLeaseCompleteAfterPublishReconcilesTopologyAndCompletesDrain() {
        keyToSegmentMap.extendMaxKeyIfNeeded(10);
        final RouteTopology<Integer> topology = mockSegmentTopology();
        final RouteTopology.RouteDrain drain = mockRouteDrain(topology);
        final MappedSegmentLeaseService<Integer, String> leaseService = new MappedSegmentLeaseService<>(keyToSegmentMap,
                segmentRegistry, topology, retryPolicy);
        when(segmentRegistry.tryGetSegment(SegmentId.of(0)))
                .thenReturn(Optional.of(blockingSegment));

        final Optional<RouteSplitLease<Integer, String>> result = leaseService.tryAcquireForSplit(SegmentId.of(0));

        assertTrue(result.isPresent());
        result.get().completeAfterPublish();
        verify(topology).reconcile(any());
        verify(drain).complete();
        verify(drain, never()).abort();
    }

    @Test
    void tryAcquireForSplitReleasesDrainWhenSegmentLoadFails() {
        keyToSegmentMap.extendMaxKeyIfNeeded(10);
        final RouteTopology<Integer> topology = mockSegmentTopology();
        final RouteTopology.RouteDrain drain = mockRouteDrain(topology);
        final MappedSegmentLeaseService<Integer, String> leaseService = new MappedSegmentLeaseService<>(keyToSegmentMap,
                segmentRegistry, topology, retryPolicy);
        final IndexException failure = new IndexException("load failed");
        when(segmentRegistry.tryGetSegment(SegmentId.of(0))).thenThrow(failure);

        final IndexException thrown = assertThrows(IndexException.class,
                () -> leaseService.tryAcquireForSplit(SegmentId.of(0)));

        assertSame(failure, thrown);
        verify(drain).abort();
        verify(drain, never()).complete();
    }

    @Test
    void acquireForWriteRetriesUnavailableRouteUntilRetryPolicyFails() {
        keyToSegmentMap.extendMaxKeyIfNeeded(10);
        segmentTopology.reconcile(keyToSegmentMap.snapshot());
        segmentTopology.tryBeginDrain(SegmentId.of(0));
        final IndexException timeout = new IndexException("timeout");
        doThrow(timeout).when(retryPolicy).backoffOrThrow(0L,
                "acquireForWrite", SegmentId.of(0));

        final IndexException thrown = assertThrows(IndexException.class,
                () -> service.acquireForWrite(10));

        assertSame(timeout, thrown);
    }

    private RouteTopology<Integer> mockTopologyStaleThenAcquired(
            final RouteMapSnapshot<Integer> snapshot) {
        final RouteTopology<Integer> topology = mockSegmentTopology();
        final RouteTopology.RouteLeaseResult stale = mock(
                RouteTopology.RouteLeaseResult.class);
        final RouteTopology.RouteLeaseResult acquired = mock(
                RouteTopology.RouteLeaseResult.class);
        final RouteTopology.RouteLease lease = mock(
                RouteTopology.RouteLease.class);
        when(topology.tryAcquire(SegmentId.of(0), snapshot.version()))
                .thenReturn(stale, acquired);
        when(stale.isAcquired()).thenReturn(false);
        when(stale.isStaleTopology()).thenReturn(true);
        when(acquired.isAcquired()).thenReturn(true);
        when(acquired.lease()).thenReturn(lease);
        when(lease.segmentId()).thenReturn(SegmentId.of(0));
        return topology;
    }

    private RouteTopology<Integer> mockTopologyWithRouteLease(
            final RouteMapSnapshot<Integer> snapshot) {
        final RouteTopology<Integer> topology = mockSegmentTopology();
        final RouteTopology.RouteLeaseResult result = mock(
                RouteTopology.RouteLeaseResult.class);
        final RouteTopology.RouteLease lease = mock(
                RouteTopology.RouteLease.class);
        when(topology.tryAcquire(SegmentId.of(0), snapshot.version()))
                .thenReturn(result);
        when(result.isAcquired()).thenReturn(true);
        when(result.lease()).thenReturn(lease);
        return topology;
    }

    private RouteTopology.RouteDrain mockRouteDrain(
            final RouteTopology<Integer> topology) {
        final RouteTopology.RouteDrain drain = mock(
                RouteTopology.RouteDrain.class);
        when(topology.tryBeginDrain(SegmentId.of(0))).thenReturn(
                Optional.of(drain));
        return drain;
    }

    @SuppressWarnings("unchecked")
    private RouteTopology<Integer> mockSegmentTopology() {
        return mock(RouteTopology.class);
    }
}
