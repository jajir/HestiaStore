package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;

@ExtendWith(MockitoExtension.class)
class SegmentSplitterTest {

    private final static SegmentId SEGMENT_ID = SegmentId.of(27);

    private final static Pair<String, String> PAIR1 = Pair.of("key1", "value1");

    private final static Pair<String, String> PAIR2 = Pair.of("key2", "value2");

    @Mock
    private Segment<String, String> segment;

    @Mock
    private Segment<String, String> lowerSegment;

    @Mock
    private SegmentFiles<String, String> segmentFiles;

    @Mock
    private VersionController versionController;

    @Mock
    private SegmentPropertiesManager segmentPropertiesManager;

    @Mock
    private SegmentDeltaCacheController<String, String> deltaCacheController;

    @Mock
    private SegmentManager<String, String> segmentManager;

    @Mock
    private SegmentStats segmentStats;

    @Mock
    private PairIterator<String, String> segmentIterator;

    @Mock
    SegmentFullWriter<String, String> segmentFullWriter;

    @Mock
    SegmentFullWriter<String, String> lowerSegmentFullWriter;

    private SegmentSplitter<String, String> splitter;

    @BeforeEach
    void setUp() {
        splitter = new SegmentSplitter<>(segment, segmentFiles,
                versionController, segmentPropertiesManager,
                deltaCacheController, segmentManager);
    }

    @Test
    void test_shouldBeCompactedBeforeSplitting_yes() {
        when(deltaCacheController.getDeltaCacheSize()).thenReturn(901);
        when(segmentPropertiesManager.getSegmentStats())
                .thenReturn(segmentStats);
        when(segmentStats.getNumberOfKeysInIndex()).thenReturn(1000L);

        assertTrue(splitter.shouldBeCompactedBeforeSplitting());
    }

    @Test
    void test_shouldBeCompactedBeforeSplitting_no() {
        when(deltaCacheController.getDeltaCacheSize()).thenReturn(100);
        when(segmentPropertiesManager.getSegmentStats())
                .thenReturn(segmentStats);
        when(segmentStats.getNumberOfKeysInIndex()).thenReturn(1000L);

        assertFalse(splitter.shouldBeCompactedBeforeSplitting());
    }

    @Test
    void test_split_segmentId_is_null() {
        final Exception err = assertThrows(NullPointerException.class, () -> {
            splitter.split(null);
        });
        assertEquals("Segment id is required", err.getMessage());
    }

    @SuppressWarnings("unchecked")
    @Test
    void test_split() {
        when(segmentPropertiesManager.getSegmentStats())
                .thenReturn(segmentStats);
        when(segmentStats.getNumberOfKeysInIndex()).thenReturn(1L);
        when(deltaCacheController.getDeltaCacheSize()).thenReturn(1);

        // main iterator behaviour
        when(segment.openIterator()).thenReturn(segmentIterator);
        when(segmentIterator.hasNext()).thenReturn(true, true, false);
        when(segmentIterator.next()).thenReturn(PAIR1, PAIR2);

        // mock writing lower part to new segment
        when(segmentManager.createSegment(SEGMENT_ID)).thenReturn(lowerSegment);
        when(lowerSegment.openFullWriter()).thenReturn(lowerSegmentFullWriter);

        // mock writing upper part to current segment
        when(segmentManager.createSegmentFullWriter())
                .thenReturn(segmentFullWriter);

        final SegmentSplitterResult<String, String> result = splitter
                .split(SEGMENT_ID);

        assertNotNull(result);
        verify(lowerSegmentFullWriter, times(1)).put(PAIR1);
        verify(lowerSegmentFullWriter, times(1)).close();
        verify(segmentFullWriter, times(1)).put(PAIR2);
        verify(segmentFullWriter, times(1)).close();
        verify(versionController, times(1)).changeVersion();
        verify(segmentIterator, times(1)).close();
    }
}