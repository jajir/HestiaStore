package org.hestiastore.index.segmentindex.core.execution;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.core.routing.MappedSegmentLease;
import org.hestiastore.index.segmentindex.core.routing.MappedSegmentLeaseService;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIteratorServiceBehaviorTest {

    @Mock
    private MappedSegmentLeaseService<String, String> segmentLeaseService;

    @Mock
    private MappedSegmentLease<String, String> lease;

    @Mock
    private MappedSegmentLease<String, String> retryLease;

    @Mock
    private BlockingSegment<String, String> segmentHandle;

    private SegmentIteratorService<String, String> service;

    @BeforeEach
    void setUp() {
        service = new SegmentIteratorService<>(
                segmentLeaseService, new BusyRetryPolicy(1, 10));
    }

    @Test
    void invalidateIterators_invalidatesLoadedMappedSegments() {
        final SegmentId segmentId = SegmentId.of(17);
        when(segmentLeaseService.getLoadedMappedSegmentIds())
                .thenReturn(List.of(segmentId));
        when(segmentLeaseService.tryAcquireLoadedMappedSegment(segmentId))
                .thenReturn(Optional.of(lease));
        when(lease.segment()).thenReturn(segmentHandle);

        service.invalidateIterators();

        verify(segmentHandle).invalidateIterators();
        verify(lease).close();
    }

    @Test
    void invalidateIterators_ignoresLookupFailureForMappedSegment() {
        final SegmentId segmentId = SegmentId.of(17);
        when(segmentLeaseService.getLoadedMappedSegmentIds())
                .thenReturn(List.of(segmentId));
        when(segmentLeaseService.tryAcquireLoadedMappedSegment(segmentId))
                .thenThrow(new IndexException("boom"));

        assertDoesNotThrow(() -> service.invalidateIterators());
    }

    @Test
    void openIterator_returnsIteratorFromCore() {
        final SegmentId segmentId = SegmentId.of(17);
        final EntryIterator<String, String> iterator = EntryIterator
                .make(List.<Entry<String, String>>of().iterator());
        when(segmentLeaseService.acquireMappedSegment(segmentId))
                .thenReturn(lease);
        when(lease.segment()).thenReturn(segmentHandle);
        when(segmentHandle.tryOpenIterator(SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(OperationResult.ok(iterator));

        final EntryIterator<String, String> result = service.openIterator(
                segmentId, SegmentIteratorIsolation.FAIL_FAST);

        assertNotNull(result);
        result.close();
        verify(segmentHandle).tryOpenIterator(
                SegmentIteratorIsolation.FAIL_FAST);
        verify(lease).close();
    }

    @Test
    void openIterator_closesLeaseWhenSegmentOpenFails() {
        final SegmentId segmentId = SegmentId.of(17);
        when(segmentLeaseService.acquireMappedSegment(segmentId))
                .thenReturn(lease);
        when(lease.segment()).thenReturn(segmentHandle);
        when(segmentHandle.tryOpenIterator(SegmentIteratorIsolation.FAIL_FAST))
                .thenThrow(new IndexException("boom"));

        assertThrows(IndexException.class, () -> service.openIterator(
                segmentId, SegmentIteratorIsolation.FAIL_FAST));

        verify(lease).close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void openIterator_retriesBusyAndFailsForError() {
        final SegmentId busySegmentId = SegmentId.of(17);
        final EntryIterator<String, String> iterator = EntryIterator
                .make(List.<Entry<String, String>>of().iterator());
        when(segmentLeaseService.acquireMappedSegment(busySegmentId))
                .thenReturn(lease, retryLease);
        when(lease.segment()).thenReturn(segmentHandle);
        when(retryLease.segment()).thenReturn(segmentHandle);
        when(segmentHandle.tryOpenIterator(
                SegmentIteratorIsolation.FAIL_FAST))
                        .thenReturn(OperationResult.busy())
                        .thenReturn(OperationResult.ok(iterator));

        final EntryIterator<String, String> result = service.openIterator(
                busySegmentId, SegmentIteratorIsolation.FAIL_FAST);
        assertNotNull(result);
        result.close();
        verify(lease).close();
        verify(retryLease).close();

        final SegmentId errorSegmentId = SegmentId.of(23);
        when(segmentLeaseService.acquireMappedSegment(errorSegmentId))
                .thenReturn(lease);
        when(segmentHandle.tryOpenIterator(SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(OperationResult.error());

        assertThrows(IndexException.class,
                () -> service.openIterator(errorSegmentId,
                        SegmentIteratorIsolation.FAIL_FAST));
    }
}
