package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentRegistrySyncAdapterTest {

    private static final SegmentId SEGMENT_ID = SegmentId.of(1);

    @Mock
    private SegmentRegistry<Integer, String> delegate;

    @Mock
    private Segment<Integer, String> segment;

    private IndexRetryPolicy retryPolicy;
    private SegmentRegistrySyncAdapter<Integer, String> adapter;

    @BeforeEach
    void setUp() {
        retryPolicy = new IndexRetryPolicy(1, 25);
        adapter = new SegmentRegistrySyncAdapter<>(delegate, retryPolicy);
    }

    @AfterEach
    void tearDown() {
        adapter = null;
        retryPolicy = null;
    }

    @Test
    void getSegment_retriesBusyUntilOk() {
        when(delegate.getSegment(SEGMENT_ID)).thenReturn(
                SegmentRegistryResult.busy(), SegmentRegistryResult.busy(),
                SegmentRegistryResult.ok(segment));

        final SegmentRegistryResult<Segment<Integer, String>> result = adapter
                .getSegment(SEGMENT_ID);

        assertEquals(SegmentRegistryResultStatus.OK, result.getStatus());
        assertSame(segment, result.getValue());
        verify(delegate, times(3)).getSegment(SEGMENT_ID);
    }

    @Test
    void getSegment_returnsClosedWithoutRetry() {
        when(delegate.getSegment(SEGMENT_ID))
                .thenReturn(SegmentRegistryResult.closed());

        final SegmentRegistryResult<Segment<Integer, String>> result = adapter
                .getSegment(SEGMENT_ID);

        assertEquals(SegmentRegistryResultStatus.CLOSED, result.getStatus());
        verify(delegate).getSegment(SEGMENT_ID);
    }

    @Test
    void getSegment_timesOutOnBusy() {
        when(delegate.getSegment(SEGMENT_ID))
                .thenReturn(SegmentRegistryResult.busy());

        final IndexException ex = assertThrows(IndexException.class,
                () -> adapter.getSegment(SEGMENT_ID));

        assertTrue(ex.getMessage().contains("timed out"));
        verify(delegate, atLeastOnce()).getSegment(SEGMENT_ID);
    }
}
