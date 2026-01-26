package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentRegistryLegacyAdapterTest {

    private static final SegmentId SEGMENT_ID = SegmentId.of(1);

    @Mock
    private SegmentRegistry<Integer, String> delegate;

    @Mock
    private Segment<Integer, String> segment;

    private SegmentRegistryLegacyAdapter<Integer, String> adapter;

    @BeforeEach
    void setUp() {
        adapter = new SegmentRegistryLegacyAdapter<>(delegate);
    }

    @AfterEach
    void tearDown() {
        adapter = null;
    }

    @Test
    void getSegment_convertsOk() {
        when(delegate.getSegment(SEGMENT_ID))
                .thenReturn(SegmentRegistryResult.ok(segment));

        final SegmentResult<Segment<Integer, String>> result = adapter
                .getSegment(SEGMENT_ID);

        assertEquals(SegmentResultStatus.OK, result.getStatus());
        assertSame(segment, result.getValue());
    }

    @Test
    void getSegment_convertsBusy() {
        when(delegate.getSegment(SEGMENT_ID))
                .thenReturn(SegmentRegistryResult.busy());

        final SegmentResult<Segment<Integer, String>> result = adapter
                .getSegment(SEGMENT_ID);

        assertEquals(SegmentResultStatus.BUSY, result.getStatus());
    }

    @Test
    void getSegment_convertsClosed() {
        when(delegate.getSegment(SEGMENT_ID))
                .thenReturn(SegmentRegistryResult.closed());

        final SegmentResult<Segment<Integer, String>> result = adapter
                .getSegment(SEGMENT_ID);

        assertEquals(SegmentResultStatus.CLOSED, result.getStatus());
    }

    @Test
    void getSegment_convertsError() {
        when(delegate.getSegment(SEGMENT_ID))
                .thenReturn(SegmentRegistryResult.error());

        final SegmentResult<Segment<Integer, String>> result = adapter
                .getSegment(SEGMENT_ID);

        assertEquals(SegmentResultStatus.ERROR, result.getStatus());
    }

    @Test
    void removeSegment_delegates() {
        adapter.removeSegment(SEGMENT_ID);

        verify(delegate).removeSegment(SEGMENT_ID);
    }

    @Test
    void close_delegates() {
        adapter.close();

        verify(delegate).close();
    }
}
