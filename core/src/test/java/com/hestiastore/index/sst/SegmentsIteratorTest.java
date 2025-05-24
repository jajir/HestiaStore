
package com.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.hestiastore.index.Pair;
import com.hestiastore.index.PairIterator;
import com.hestiastore.index.segment.Segment;
import com.hestiastore.index.segment.SegmentId;

@ExtendWith(MockitoExtension.class)
class SegmentsIteratorTest {

    private final static SegmentId SEGMENT_ID_17 = SegmentId.of(17);
    private final static SegmentId SEGMENT_ID_23 = SegmentId.of(23);

    @Mock
    private SegmentManager<String, String> segmentManager;

    @Mock
    private Segment<String, String> segment17;

    @Mock
    private Segment<String, String> segment23;

    @Mock
    private PairIterator<String, String> pairIterator17;

    @Mock
    private PairIterator<String, String> pairIterator23;

    @Test
    void test_there_is_no_segment() {
        try (SegmentsIterator<String, String> iterator = new SegmentsIterator<>(
                new ArrayList<>(), segmentManager)) {
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_segments_in_one() {
        when(segmentManager.getSegment(SEGMENT_ID_17)).thenReturn(segment17);
        when(segment17.openIterator()).thenReturn(pairIterator17);
        when(pairIterator17.hasNext()).thenReturn(true, false);
        when(pairIterator17.next()).thenReturn(new Pair<>("key1", "value1"));

        final ArrayList<SegmentId> tst = new ArrayList<SegmentId>();
        tst.add(SEGMENT_ID_17);

        try (SegmentsIterator<String, String> iterator = new SegmentsIterator<>(
                tst, segmentManager)) {
            assertTrue(iterator.hasNext());
            final Pair<String, String> pair1 = iterator.next();
            assertEquals("key1", pair1.getKey());
            assertEquals("value1", pair1.getValue());

            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_segments_are_two() {
        when(segmentManager.getSegment(SEGMENT_ID_17)).thenReturn(segment17);
        when(segment17.openIterator()).thenReturn(pairIterator17);
        when(pairIterator17.hasNext()).thenReturn(true, false);
        when(pairIterator17.next()).thenReturn(new Pair<>("key1", "value1"));

        when(segmentManager.getSegment(SEGMENT_ID_23)).thenReturn(segment23);
        when(segment23.openIterator()).thenReturn(pairIterator23);
        when(pairIterator23.hasNext()).thenReturn(true, false);
        when(pairIterator23.next()).thenReturn(new Pair<>("key2", "value2"));

        final ArrayList<SegmentId> tst = new ArrayList<SegmentId>();
        tst.add(SEGMENT_ID_17);
        tst.add(SEGMENT_ID_23);

        try (SegmentsIterator<String, String> iterator = new SegmentsIterator<>(
                tst, segmentManager)) {
            assertTrue(iterator.hasNext());
            final Pair<String, String> pair1 = iterator.next();
            assertEquals("key1", pair1.getKey());
            assertEquals("value1", pair1.getValue());

            assertTrue(iterator.hasNext());
            final Pair<String, String> pair2 = iterator.next();
            assertEquals("key2", pair2.getKey());
            assertEquals("value2", pair2.getValue());

            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void testClose() {
        when(segmentManager.getSegment(SEGMENT_ID_17)).thenReturn(segment17);
        when(segment17.openIterator()).thenReturn(pairIterator17);

        final ArrayList<SegmentId> tst = new ArrayList<SegmentId>();
        tst.add(SEGMENT_ID_17);

        SegmentsIterator<String, String> iterator = new SegmentsIterator<>(tst,
                segmentManager);
        iterator.close();

        verify(pairIterator17, atLeastOnce()).close();
        assertFalse(iterator.hasNext());
    }
}