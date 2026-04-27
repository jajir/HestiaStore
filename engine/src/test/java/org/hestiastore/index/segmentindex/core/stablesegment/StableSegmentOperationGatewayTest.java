package org.hestiastore.index.segmentindex.core.stablesegment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StableSegmentOperationGatewayTest {

    @Mock
    private SegmentRegistry<String, String> segmentRegistry;

    @Mock
    private BlockingSegment<String, String> segmentHandle;

    private StableSegmentOperationGateway<String, String> stableSegmentGateway;

    @BeforeEach
    void setUp() {
        stableSegmentGateway = new StableSegmentOperationGateway<>(
                segmentRegistry);
    }

    @Test
    void openIterator_returnsValueWhenOk() {
        final SegmentId segmentId = segmentId();
        final EntryIterator<String, String> iterator = EntryIterator
                .make(List.<Entry<String, String>>of().iterator());
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.tryOpenIterator(SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(OperationResult.ok(iterator));

        final StableSegmentOperationResult<EntryIterator<String, String>> result =
                stableSegmentGateway.openIterator(segmentId,
                        SegmentIteratorIsolation.FAIL_FAST);

        assertEquals(StableSegmentOperationStatus.OK, result.getStatus());
        assertSame(iterator, result.getValue());
    }

    @Test
    void flush_mapsClosedStatusWithoutLosingSignal() {
        final SegmentId segmentId = segmentId();
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.tryFlush()).thenReturn(OperationResult.closed());

        final StableSegmentOperationResult<BlockingSegment<String, String>> result =
                stableSegmentGateway.flush(segmentId);

        assertEquals(StableSegmentOperationStatus.CLOSED, result.getStatus());
    }

    private SegmentId segmentId() {
        return SegmentId.of(0);
    }
}
