package org.hestiastore.index.segmentindex.core.routing;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.core.routing.RouteTopology.RouteDrain;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentindex.routemap.RouteMapSnapshot;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RouteSplitLeaseTest {

    @Mock
    private RouteDrain routeDrain;

    @Mock
    private SegmentRouteMap<Integer> keyToSegmentMap;

    @Mock
    private RouteTopology<Integer> segmentTopology;

    @Mock
    private BlockingSegment<Integer, String> segment;

    @Mock
    private RouteMapSnapshot<Integer> snapshot;

    private RouteSplitLease<Integer, String> lease;

    @BeforeEach
    void setUp() {
        lease = new RouteSplitLease<>(routeDrain, keyToSegmentMap,
                segmentTopology, segment);
    }

    @Test
    void segmentIdReturnsLoadedSegmentId() {
        when(segment.getId()).thenReturn(SegmentId.of(3));

        assertEquals(SegmentId.of(3), lease.segmentId());
    }

    @Test
    void segmentReturnsLoadedSegment() {
        assertSame(segment, lease.segment());
    }

    @Test
    void completeAfterPublishReconcilesTopologyAndCompletesDrain() {
        when(keyToSegmentMap.snapshot()).thenReturn(snapshot);

        assertDoesNotThrow(() -> lease.completeAfterPublish());

        verify(segmentTopology).reconcile(snapshot);
        verify(routeDrain).complete();
    }

    @Test
    void completeAfterPublishCompletesDrainWhenReconcileFails() {
        final IllegalStateException failure = new IllegalStateException(
                "reconcile failed");
        when(keyToSegmentMap.snapshot()).thenReturn(snapshot);
        doThrow(failure).when(segmentTopology).reconcile(snapshot);

        final IllegalStateException thrown = assertThrows(
                IllegalStateException.class, lease::completeAfterPublish);

        assertSame(failure, thrown);
        verify(routeDrain).complete();
    }

    @Test
    void abortIsIdempotent() {
        assertDoesNotThrow(() -> {
            lease.abort();
            lease.abort();
        });

        verify(routeDrain).abort();
        verifyNoMoreInteractions(routeDrain);
    }

    @Test
    void closeAbortsDrain() {
        assertDoesNotThrow(() -> lease.close());

        verify(routeDrain).abort();
    }
}
