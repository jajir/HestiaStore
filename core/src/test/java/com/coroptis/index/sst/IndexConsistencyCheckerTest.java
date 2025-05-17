package com.coroptis.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.IndexException;
import com.coroptis.index.LoggingContext;
import com.coroptis.index.Pair;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.segment.Segment;
import com.coroptis.index.segment.SegmentId;

@ExtendWith(MockitoExtension.class)
public class IndexConsistencyCheckerTest {

    private final static LoggingContext LOGGING_CONTEXT = new LoggingContext(
            "test_index");
    private final static TypeDescriptor<Integer> TYPE_DESCRIPTOR_INTEGER = new TypeDescriptorInteger();
    private final static SegmentId SEGMENT_ID = SegmentId.of(13);
    private final static Integer SEGMENT_MAX_KEY = 73;

    @Mock
    private KeySegmentCache<Integer> keySegmentCache;

    @Mock
    private SegmentManager<Integer, String> segmentManager;

    @Mock
    private Segment<Integer, String> segment;

    private Pair<Integer, SegmentId> segmentPair;

    private IndexConsistencyChecker<Integer, String> checker;

    @Test
    void test_noSegments() throws Exception {
        when(keySegmentCache.getSegmentsAsStream()).thenReturn(Stream.empty());

        checker.checkAndRepairConsistency();

        assertTrue(true);
    }

    @Test
    void test_missingSegment() throws Exception {
        when(keySegmentCache.getSegmentsAsStream())
                .thenReturn(Stream.of(segmentPair));
        when(segmentManager.getSegment(SEGMENT_ID)).thenReturn(null);

        final Exception e = assertThrows(IndexException.class,
                () -> checker.checkAndRepairConsistency());

        assertEquals("Segment 'segment-00013' is not found in index.",
                e.getMessage());
    }

    @Test
    void test_oneSegment_segmenMaxKeyIsHigher() throws Exception {
        when(keySegmentCache.getSegmentsAsStream())
                .thenReturn(Stream.of(segmentPair));
        when(segmentManager.getSegment(SEGMENT_ID)).thenReturn(segment);
        when(segment.checkAndRepairConsistency())
                .thenReturn(SEGMENT_MAX_KEY + 1);

        checker.checkAndRepairConsistency();

        assertTrue(true);
    }

    @Test
    void test_oneSegment() throws Exception {
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
        checker = new IndexConsistencyChecker<>(LOGGING_CONTEXT,
                keySegmentCache, segmentManager, TYPE_DESCRIPTOR_INTEGER);
        segmentPair = Pair.of(SEGMENT_MAX_KEY, SEGMENT_ID);
    }

    @AfterEach
    void tearDown() {
        checker = null;
        segmentPair = null;
    }
}
