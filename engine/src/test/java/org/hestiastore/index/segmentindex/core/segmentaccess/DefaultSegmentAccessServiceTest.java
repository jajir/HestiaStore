package org.hestiastore.index.segmentindex.core.segmentaccess;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapImpl;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.mapping.Snapshot;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultSegmentAccessServiceTest {

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private BlockingSegment<Integer, String> blockingSegment;

    @Mock
    private IndexRetryPolicy retryPolicy;

    private KeyToSegmentMap<Integer> keyToSegmentMap;
    private SegmentTopology<Integer> segmentTopology;
    private SegmentAccessService<Integer, String> service;

    @BeforeEach
    void setUp() {
        keyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                new KeyToSegmentMapImpl<>(new MemDirectory(),
                        new TypeDescriptorInteger()));
        segmentTopology = SegmentTopology.<Integer>builder()
                .snapshot(keyToSegmentMap.snapshot()).build();
        service = new DefaultSegmentAccessService<>(keyToSegmentMap,
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
        final SegmentAccess<Integer, String> result = service
                .acquireForRead(10);

        assertNull(result);
        verifyNoInteractions(segmentRegistry);
    }

    @Test
    void acquireForWriteCreatesBootstrapRouteAndReturnsAccess() {
        when(segmentRegistry.loadSegment(SegmentId.of(0)))
                .thenReturn(blockingSegment);

        try (SegmentAccess<Integer, String> access = service
                .acquireForWrite(11)) {
            assertSame(blockingSegment, access.segment());
            assertEquals(SegmentId.of(0), access.segmentId());
        }

        assertEquals(SegmentId.of(0),
                keyToSegmentMap.findSegmentIdForKey(11));
        verify(segmentRegistry).loadSegment(SegmentId.of(0));
    }

    @Test
    void acquireForWriteProvidesSegmentForCallerOperation() {
        when(segmentRegistry.loadSegment(SegmentId.of(0)))
                .thenReturn(blockingSegment);

        try (SegmentAccess<Integer, String> access = service
                .acquireForWrite(11)) {
            access.segment().put(11, "v11");
        }

        verify(blockingSegment).put(11, "v11");
    }

    @Test
    void acquireForWriteClosesRouteLeaseWhenSegmentLoadFails() {
        keyToSegmentMap.extendMaxKeyIfNeeded(10);
        final Snapshot<Integer> snapshot = keyToSegmentMap.snapshot();
        @SuppressWarnings("unchecked")
        final SegmentTopology<Integer> topology = mock(SegmentTopology.class);
        final SegmentTopology.RouteLeaseResult result = mock(
                SegmentTopology.RouteLeaseResult.class);
        final SegmentTopology.RouteLease lease = mock(
                SegmentTopology.RouteLease.class);
        when(topology.tryAcquire(SegmentId.of(0), snapshot.version()))
                .thenReturn(result);
        when(result.isAcquired()).thenReturn(true);
        when(result.lease()).thenReturn(lease);
        when(lease.segmentId()).thenReturn(SegmentId.of(0));
        final SegmentAccessService<Integer, String> accessService =
                new DefaultSegmentAccessService<>(keyToSegmentMap,
                        segmentRegistry, topology, retryPolicy);
        final IllegalStateException failure = new IllegalStateException(
                "failed");
        when(segmentRegistry.loadSegment(SegmentId.of(0))).thenThrow(failure);

        assertThrows(IllegalStateException.class,
                () -> accessService.acquireForWrite(10));

        verify(lease).close();
    }

    @Test
    void acquireForReadReconcilesStaleTopologyAndRetries() {
        keyToSegmentMap.extendMaxKeyIfNeeded(10);
        final Snapshot<Integer> snapshot = keyToSegmentMap.snapshot();
        final SegmentTopology<Integer> topology = mockTopologyStaleThenAcquired(
                snapshot);
        final SegmentAccessService<Integer, String> accessService =
                new DefaultSegmentAccessService<>(keyToSegmentMap,
                        segmentRegistry, topology, retryPolicy);
        when(segmentRegistry.loadSegment(SegmentId.of(0)))
                .thenReturn(blockingSegment);

        try (SegmentAccess<Integer, String> access = accessService
                .acquireForRead(10)) {
            assertSame(blockingSegment, access.segment());
        }

        verify(topology).reconcile(any());
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

    private SegmentTopology<Integer> mockTopologyStaleThenAcquired(
            final Snapshot<Integer> snapshot) {
        @SuppressWarnings("unchecked")
        final SegmentTopology<Integer> topology = mock(SegmentTopology.class);
        final SegmentTopology.RouteLeaseResult stale = mock(
                SegmentTopology.RouteLeaseResult.class);
        final SegmentTopology.RouteLeaseResult acquired = mock(
                SegmentTopology.RouteLeaseResult.class);
        final SegmentTopology.RouteLease lease = mock(
                SegmentTopology.RouteLease.class);
        when(topology.tryAcquire(SegmentId.of(0), snapshot.version()))
                .thenReturn(stale, acquired);
        when(stale.isAcquired()).thenReturn(false);
        when(stale.isStaleTopology()).thenReturn(true);
        when(acquired.isAcquired()).thenReturn(true);
        when(acquired.lease()).thenReturn(lease);
        when(lease.segmentId()).thenReturn(SegmentId.of(0));
        return topology;
    }
}
