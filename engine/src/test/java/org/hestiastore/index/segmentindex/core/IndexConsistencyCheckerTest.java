package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.hestiastore.index.segmentregistry.SegmentRegistryResultStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexConsistencyCheckerTest {

    @Mock
    private Segment<Integer, String> segment;
    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;
    @Mock
    private KeyToSegmentMap<Integer> keyToSegmentMap;
    @Mock
    private EntryIterator<Integer, String> iterator;

    private KeyToSegmentMapSynchronizedAdapter<Integer> synchronizedKeyToSegmentMap;

    private IndexConsistencyChecker<Integer, String> checker;

    @BeforeEach
    void setUp() {
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
        checker = new IndexConsistencyChecker<>(synchronizedKeyToSegmentMap,
                segmentRegistry, new TypeDescriptorInteger());
    }

    @AfterEach
    void tearDown() {
        checker = null;
        synchronizedKeyToSegmentMap = null;
    }

    @Test
    void emptySegmentIsRemovedAfterIsolationCheck() {
        when(segment.checkAndRepairConsistency()).thenReturn(null);
        when(segment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(SegmentResult.ok(iterator));
        when(iterator.hasNext()).thenReturn(false);
        when(keyToSegmentMap.getSegmentsAsStream())
                .thenReturn(Stream.of(Entry.of(10, SegmentId.of(1))));
        when(segmentRegistry.getSegment(SegmentId.of(1)))
                .thenReturn(SegmentRegistryResult.ok(segment));

        checker.checkAndRepairConsistency();

        verify(keyToSegmentMap).removeSegment(SegmentId.of(1));
        verify(keyToSegmentMap).optionallyFlush();
        verify(segmentRegistry).deleteSegment(SegmentId.of(1));
    }

    @Test
    void busySegmentLoad_retriesUntilSegmentIsAvailable() {
        when(segment.checkAndRepairConsistency()).thenReturn(10);
        when(keyToSegmentMap.getSegmentsAsStream())
                .thenReturn(Stream.of(Entry.of(10, SegmentId.of(1))));
        when(segmentRegistry.getSegment(SegmentId.of(1)))
                .thenReturn(SegmentRegistryResult
                        .fromStatus(SegmentRegistryResultStatus.BUSY))
                .thenReturn(SegmentRegistryResult.ok(segment));

        checker = new IndexConsistencyChecker<>(synchronizedKeyToSegmentMap,
                segmentRegistry, new TypeDescriptorInteger(),
                segmentId -> true, new IndexRetryPolicy(1, 20));

        checker.checkAndRepairConsistency();

        verify(segmentRegistry, times(2)).getSegment(SegmentId.of(1));
    }

    @Test
    void busyIsolationIterator_retriesUntilOpened() {
        when(segment.checkAndRepairConsistency()).thenReturn(null);
        when(segment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(SegmentResult.busy())
                .thenReturn(SegmentResult.ok(iterator));
        when(iterator.hasNext()).thenReturn(false);
        when(keyToSegmentMap.getSegmentsAsStream())
                .thenReturn(Stream.of(Entry.of(10, SegmentId.of(1))));
        when(segmentRegistry.getSegment(SegmentId.of(1)))
                .thenReturn(SegmentRegistryResult.ok(segment));

        checker = new IndexConsistencyChecker<>(synchronizedKeyToSegmentMap,
                segmentRegistry, new TypeDescriptorInteger(),
                segmentId -> true, new IndexRetryPolicy(1, 20));

        checker.checkAndRepairConsistency();

        verify(segment, times(2))
                .openIterator(SegmentIteratorIsolation.FULL_ISOLATION);
    }

    @Test
    void busySegmentLoad_timesOutWithRetryPolicyMessage() {
        when(keyToSegmentMap.getSegmentsAsStream())
                .thenReturn(Stream.of(Entry.of(10, SegmentId.of(1))));
        when(segmentRegistry.getSegment(SegmentId.of(1)))
                .thenReturn(SegmentRegistryResult
                        .fromStatus(SegmentRegistryResultStatus.BUSY));

        checker = new IndexConsistencyChecker<>(synchronizedKeyToSegmentMap,
                segmentRegistry, new TypeDescriptorInteger(),
                segmentId -> true, new IndexRetryPolicy(1, 1));

        final IndexException ex = assertThrows(IndexException.class,
                () -> checker.checkAndRepairConsistency());

        verify(segmentRegistry, times(1)).getSegment(SegmentId.of(1));
        assertTrue(ex.getMessage()
                .contains("loadSegmentForConsistency"));
    }
}
