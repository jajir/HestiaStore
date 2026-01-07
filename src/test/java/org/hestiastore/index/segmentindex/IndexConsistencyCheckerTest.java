package org.hestiastore.index.segmentindex;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentWriteLockSupport;
import org.junit.jupiter.api.Test;

class IndexConsistencyCheckerTest {

    @Test
    void emptySegmentIsRemovedUnderWriteLock() {
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = (Segment<Integer, String>) mock(
                Segment.class, withSettings()
                        .extraInterfaces(SegmentWriteLockSupport.class));
        when(segment.checkAndRepairConsistency()).thenReturn(null);

        final SegmentWriteLockSupport<Integer, String> lockingSupport = (SegmentWriteLockSupport<Integer, String>) segment;
        when(lockingSupport.executeWithWriteLock(any())).thenAnswer(invocation -> {
            return invocation.<java.util.function.Supplier<?>>getArgument(0)
                    .get();
        });

        final SegmentRegistry<Integer, String> segmentRegistry = mock(
                SegmentRegistry.class);
        final KeySegmentCache<Integer> keySegmentCache = mock(
                KeySegmentCache.class);
        when(keySegmentCache.getSegmentsAsStream())
                .thenReturn(Stream.of(
                        Entry.of(10, SegmentId.of(1))));
        when(segmentRegistry.getSegment(SegmentId.of(1))).thenReturn(segment);

        final IndexConsistencyChecker<Integer, String> checker = new IndexConsistencyChecker<>(
                keySegmentCache, segmentRegistry, new TypeDescriptorInteger());

        checker.checkAndRepairConsistency();

        verify(keySegmentCache).removeSegment(SegmentId.of(1));
        verify(keySegmentCache).optionalyFlush();
        verify(segmentRegistry).removeSegment(SegmentId.of(1));
    }
}
