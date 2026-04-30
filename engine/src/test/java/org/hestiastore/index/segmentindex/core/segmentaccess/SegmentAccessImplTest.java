package org.hestiastore.index.segmentindex.core.segmentaccess;

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
class SegmentAccessImplTest {

    @Mock
    private RouteLease routeLease;

    @Mock
    private BlockingSegment<Integer, String> segment;

    private SegmentAccessImpl<Integer, String> access;

    @BeforeEach
    void setUp() {
        access = new SegmentAccessImpl<>(routeLease, segment);
    }

    @Test
    void segmentIdReturnsRouteLeaseSegmentId() {
        when(routeLease.segmentId()).thenReturn(SegmentId.of(7));

        assertEquals(SegmentId.of(7), access.segmentId());
    }

    @Test
    void segmentReturnsLoadedSegment() {
        assertSame(segment, access.segment());
    }

    @Test
    void closeClosesRouteLease() {
        access.close();

        verify(routeLease).close();
    }
}
