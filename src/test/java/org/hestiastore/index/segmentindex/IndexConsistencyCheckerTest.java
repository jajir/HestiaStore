package org.hestiastore.index.segmentindex;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
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
                .thenReturn(Stream.of(
                        Entry.of(10, SegmentId.of(1))));
        when(segmentRegistry.getSegment(SegmentId.of(1)))
                .thenReturn(SegmentRegistryResult.ok(segment));

        checker.checkAndRepairConsistency();

        verify(keyToSegmentMap).removeSegment(SegmentId.of(1));
        verify(keyToSegmentMap).optionalyFlush();
        verify(segmentRegistry).removeSegment(SegmentId.of(1));
    }
}
