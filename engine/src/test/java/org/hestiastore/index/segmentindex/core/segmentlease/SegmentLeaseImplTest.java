package org.hestiastore.index.segmentindex.core.segmentlease;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology.RouteLease;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentLeaseImplTest {

    @Mock
    private RouteLease routeLease;

    @Mock
    private BlockingSegment<Integer, String> segment;

    private SegmentLeaseImpl<Integer, String> lease;

    @BeforeEach
    void setUp() {
        lease = new SegmentLeaseImpl<>(routeLease, segment);
    }

    @Test
    void segmentIdReturnsRouteLeaseSegmentId() {
        when(routeLease.segmentId()).thenReturn(SegmentId.of(7));

        assertEquals(SegmentId.of(7), lease.segmentId());
    }

    @Test
    void segmentReturnsLoadedSegment() {
        assertSame(segment, lease.segment());
    }

    @Test
    void closeClosesRouteLease() {
        lease.close();

        verify(routeLease).close();
    }
}
