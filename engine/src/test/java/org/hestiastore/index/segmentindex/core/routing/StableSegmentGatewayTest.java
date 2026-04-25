package org.hestiastore.index.segmentindex.core.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StableSegmentGatewayTest {

    @Mock
    private SegmentRegistry<String, String> segmentRegistry;

    @Mock
    private SegmentHandle<String, String> segmentHandle;

    private StableSegmentGateway<String, String> stableSegmentGateway;

    @BeforeEach
    void setUp() {
        stableSegmentGateway = new StableSegmentGateway<>(segmentRegistry);
    }

    @Test
    void get_returnsBusyWhenSegmentIsBusy() {
        final SegmentId segmentId = segmentId();
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.tryGet("key")).thenReturn(SegmentResult.busy());

        final IndexResult<String> result = stableSegmentGateway.get(segmentId,
                "key");

        assertEquals(IndexResultStatus.BUSY, result.getStatus());
    }

    @Test
    void get_returnsOkWhenMappingAndTopologyStayStable() {
        final SegmentId segmentId = segmentId();
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.tryGet("key")).thenReturn(SegmentResult.ok("value"));

        final IndexResult<String> result = stableSegmentGateway.get(segmentId,
                "key");

        assertEquals(IndexResultStatus.OK, result.getStatus());
        assertEquals("value", result.getValue());
    }

    @Test
    void put_returnsOkWhenSegmentAcceptsWrite() {
        final SegmentId segmentId = segmentId();
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.tryPut("key", "value"))
                .thenReturn(SegmentResult.ok());

        final IndexResult<Void> result = stableSegmentGateway.put(segmentId,
                "key", "value");

        assertEquals(IndexResultStatus.OK, result.getStatus());
    }

    @Test
    void put_returnsBusyWhenSegmentRejectsWrite() {
        final SegmentId segmentId = segmentId();
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.tryPut("key", "value"))
                .thenReturn(SegmentResult.busy());

        final IndexResult<Void> result = stableSegmentGateway.put(segmentId,
                "key", "value");

        assertEquals(IndexResultStatus.BUSY, result.getStatus());
    }

    @Test
    void put_returnsBusyWhenSegmentHandleIsNotLoaded() {
        final SegmentId segmentId = segmentId();
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.empty());

        final IndexResult<Void> result = stableSegmentGateway.put(segmentId,
                "key", "value");

        assertEquals(IndexResultStatus.BUSY, result.getStatus());
    }

    @Test
    void openIterator_returnsValueWhenOk() {
        final SegmentId segmentId = segmentId();
        final EntryIterator<String, String> iterator = EntryIterator
                .make(List.<Entry<String, String>>of().iterator());
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.tryOpenIterator(SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(SegmentResult.ok(iterator));

        final IndexResult<EntryIterator<String, String>> result = stableSegmentGateway
                .openIterator(segmentId, SegmentIteratorIsolation.FAIL_FAST);

        assertEquals(IndexResultStatus.OK, result.getStatus());
        assertSame(iterator, result.getValue());
    }

    @Test
    void flush_mapsClosedStatusWithoutLosingSignal() {
        final SegmentId segmentId = segmentId();
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.tryFlush()).thenReturn(SegmentResult.closed());

        final IndexResult<SegmentHandle<String, String>> result =
                stableSegmentGateway.flush(segmentId);

        assertEquals(IndexResultStatus.CLOSED, result.getStatus());
    }

    private SegmentId segmentId() {
        return SegmentId.of(0);
    }
}
