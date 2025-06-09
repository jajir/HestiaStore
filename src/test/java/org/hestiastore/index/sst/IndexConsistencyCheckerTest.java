package org.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Pair;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class IndexConsistencyCheckerTest {

    private static final TypeDescriptor<Integer> TYPE_DESCRIPTOR_INTEGER = new TypeDescriptorInteger();
    private static final SegmentId SEGMENT_ID = SegmentId.of(13);
    private static final Integer SEGMENT_MAX_KEY = 73;

    @Mock
    private KeySegmentCache<Integer> keySegmentCache;

    @Mock
    private SegmentManager<Integer, String> segmentManager;

    @Mock
    private Segment<Integer, String> segment;

    private Pair<Integer, SegmentId> segmentPair;

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
        when(segmentManager.getSegment(SEGMENT_ID)).thenReturn(null);

        final Exception e = assertThrows(IndexException.class,
                () -> checker.checkAndRepairConsistency());

        assertEquals("Segment 'segment-00013' is not found in index.",
                e.getMessage());
    }

    @Test
    void test_oneSegment_segmenMaxKeyIsHigher() {
        when(keySegmentCache.getSegmentsAsStream())
                .thenReturn(Stream.of(segmentPair));
        when(segmentManager.getSegment(SEGMENT_ID)).thenReturn(segment);
        when(segment.checkAndRepairConsistency())
                .thenReturn(SEGMENT_MAX_KEY + 1);

        checker.checkAndRepairConsistency();

        assertTrue(true);
    }

    @Test
    void test_oneSegment() {
        when(keySegmentCache.getSegmentsAsStream())
                .thenReturn(Stream.of(segmentPair));
        when(segmentManager.getSegment(SEGMENT_ID)).thenReturn(segment);
        when(segment.checkAndRepairConsistency()).thenReturn(SEGMENT_MAX_KEY);

        checker.checkAndRepairConsistency();

        assertTrue(true);
    }

    // no such segment

    @BeforeEach
    void setUp() {
        checker = new IndexConsistencyChecker<>(keySegmentCache, segmentManager,
                TYPE_DESCRIPTOR_INTEGER);
        segmentPair = Pair.of(SEGMENT_MAX_KEY, SEGMENT_ID);
    }

    @AfterEach
    void tearDown() {
        checker = null;
        segmentPair = null;
    }
}
