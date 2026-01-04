package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentasync.SegmentAsync;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexConsistencyCheckerTest {

    private static final TypeDescriptor<Integer> TYPE_DESCRIPTOR_INTEGER = new TypeDescriptorInteger();
    private static final SegmentId SEGMENT_ID = SegmentId.of(13);
    private static final Integer SEGMENT_MAX_KEY = 73;

    @Mock
    private KeySegmentCache<Integer> keySegmentCache;

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private SegmentAsync<Integer, String> segment;

    private Entry<Integer, SegmentId> segmentPair;

    private IndexConsistencyChecker<Integer, String> checker;

    @Test
    void test_noSegments() {
        when(keySegmentCache.getSegmentsAsStream()).thenReturn(Stream.empty());

        checker.checkAndRepairConsistency();

        assertTrue(true);
    }

    @Test
    void test_missingSegment() {
        when(keySegmentCache.getSegmentsAsStream())
                .thenReturn(Stream.of(segmentPair));
        when(segmentRegistry.getSegment(SEGMENT_ID)).thenReturn(null);

        final Exception e = assertThrows(IndexException.class,
                () -> checker.checkAndRepairConsistency());

        assertEquals(
                "Index is broken. File 'index.map' "
                        + "containing information about segments is corrupted. "
                        + "Segment 'segment-00013' is not found in index.",
                e.getMessage());
    }

    @Test
    void test_oneSegment_segmentKey_is_null() {
        when(keySegmentCache.getSegmentsAsStream())
                .thenReturn(Stream.of(Entry.of(null, SEGMENT_ID)));

        final Exception e = assertThrows(IndexException.class,
                () -> checker.checkAndRepairConsistency());

        assertEquals(
                "Index is broken. " + "File 'index.map' containing information "
                        + "about segments is corrupted. Segment key is null.",
                e.getMessage());
    }

    @Test
    void test_oneSegment_segmentId_is_null() {
        when(keySegmentCache.getSegmentsAsStream())
                .thenReturn(Stream.of(Entry.of(SEGMENT_MAX_KEY, null)));

        final Exception e = assertThrows(IndexException.class,
                () -> checker.checkAndRepairConsistency());

        assertEquals(
                "Index is broken. " + "File 'index.map' containing information "
                        + "about segments is corrupted. Segment id is null.",
                e.getMessage());
    }

    @Test
    void test_oneSegment_segmentMaxKey_is_null_removes_segment() {
        when(keySegmentCache.getSegmentsAsStream())
                .thenReturn(Stream.of(segmentPair));
        when(segmentRegistry.getSegment(SEGMENT_ID)).thenReturn(segment);
        when(segment.checkAndRepairConsistency()).thenReturn(null);

        checker.checkAndRepairConsistency();

        verify(segmentRegistry).removeSegment(SEGMENT_ID);
        verify(keySegmentCache).removeSegment(SEGMENT_ID);
        verify(keySegmentCache).optionalyFlush();
    }

    @Test
    void test_oneSegment_segmentMaxKeyIsHigher() {
        when(keySegmentCache.getSegmentsAsStream())
                .thenReturn(Stream.of(segmentPair));
        when(segmentRegistry.getSegment(SEGMENT_ID)).thenReturn(segment);
        when(segment.checkAndRepairConsistency())
                .thenReturn(SEGMENT_MAX_KEY + 1);

        final Exception e = assertThrows(IndexException.class,
                () -> checker.checkAndRepairConsistency());

        assertEquals("Index is broken. File 'index.map' containing information "
                + "about segments is corrupted. Segment 'segment-00013' "
                + "has a max key of '73', which is less "
                + "than the max key '74' from the index data.", e.getMessage());
    }

    @Test
    void test_oneSegment() {
        when(keySegmentCache.getSegmentsAsStream())
                .thenReturn(Stream.of(segmentPair));
        when(segmentRegistry.getSegment(SEGMENT_ID)).thenReturn(segment);
        when(segment.checkAndRepairConsistency()).thenReturn(SEGMENT_MAX_KEY);

        checker.checkAndRepairConsistency();

        assertTrue(true);
    }

    // no such segment

    @BeforeEach
    void setUp() {
        checker = new IndexConsistencyChecker<>(keySegmentCache,
                segmentRegistry, TYPE_DESCRIPTOR_INTEGER);
        segmentPair = Entry.of(SEGMENT_MAX_KEY, SEGMENT_ID);
    }

    @AfterEach
    void tearDown() {
        checker = null;
        segmentPair = null;
    }
}
