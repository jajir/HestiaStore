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

    private final static Pair<String, String> PAIR3 = Pair.of("key3", "value3");

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
    void test_shouldBeCompactedBeforeSplitting_yes_lowEstimatedNumberOfKeys() {
        when(segmentPropertiesManager.getSegmentStats())
                .thenReturn(segmentStats);
        when(segmentStats.getNumberOfKeysInSegment()).thenReturn(2L);
        when(deltaCacheController.getDeltaCacheSizeWithoutTombstones())
                .thenReturn(1);

        assertTrue(splitter.shouldBeCompactedBeforeSplitting(1000));
    }

    @Test
    void test_shouldBeCompactedBeforeSplitting_no() {
        when(segmentPropertiesManager.getSegmentStats())
                .thenReturn(segmentStats);
        when(segmentStats.getNumberOfKeysInSegment()).thenReturn(1000L);
        when(deltaCacheController.getDeltaCacheSizeWithoutTombstones())
                .thenReturn(100);

        assertFalse(splitter.shouldBeCompactedBeforeSplitting(1000));
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
        when(segmentStats.getNumberOfKeysInSegment()).thenReturn(2L);
        when(deltaCacheController.getDeltaCacheSizeWithoutTombstones())
                .thenReturn(2);

        // main iterator behaviour
        when(segment.openIterator()).thenReturn(segmentIterator);
        when(segmentIterator.hasNext()).thenReturn(true, true, true, false);
        when(segmentIterator.next()).thenReturn(PAIR1, PAIR2, PAIR3);

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
        verify(lowerSegmentFullWriter, times(1)).put(PAIR2);
        verify(lowerSegmentFullWriter, times(1)).close();
        verify(segmentFullWriter, times(1)).put(PAIR3);
        verify(segmentFullWriter, times(1)).close();
        verify(versionController, times(1)).changeVersion();
        verify(segmentIterator, times(1)).close();
    }

    @Test
    void test_split_half_is_zero() {
        when(segmentPropertiesManager.getSegmentStats())
                .thenReturn(segmentStats);
        when(segmentStats.getNumberOfKeysInSegment()).thenReturn(0L);
        when(deltaCacheController.getDeltaCacheSizeWithoutTombstones())
                .thenReturn(1);

        final Exception err = assertThrows(IllegalStateException.class,
                () -> splitter.split(SEGMENT_ID));
        assertEquals("Splitting failed. Number of keys is too low.",
                err.getMessage());
    }
}