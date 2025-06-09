package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentCompacterTest {

    @Mock
    private Segment<Integer, String> segment;

    @Mock
    private SegmentFiles<Integer, String> segmentFiles;

    @Mock
    private SegmentConf segmentConf;

    @Mock
    private VersionController versionController;

    @Mock
    private SegmentPropertiesManager segmentPropertiesManager;

    private SegmentCompacter<Integer, String> sc;

    @BeforeEach
    void setUp() {
        sc = new SegmentCompacter<>(segment, segmentFiles, segmentConf,
                versionController, segmentPropertiesManager);
    }

    @Test
    void test_basic_operations() {
        assertNotNull(sc);
    }

    @Test
    void test_shouldBeCompactedDuringWriting_yes() {
        when(segmentPropertiesManager.getSegmentStats())
                .thenReturn(new SegmentStats(10, 1000L, 15));
        when(segmentConf.getMaxNumberOfKeysInDeltaCacheDuringWriting())
                .thenReturn(20L);

        assertTrue(sc.shouldBeCompactedDuringWriting(25));
    }

    @Test
    void test_shouldBeCompactedDuringWriting_no() {
        when(segmentPropertiesManager.getSegmentStats())
                .thenReturn(new SegmentStats(10, 1000L, 15));
        when(segmentConf.getMaxNumberOfKeysInDeltaCacheDuringWriting())
                .thenReturn(30L);

        assertFalse(sc.shouldBeCompactedDuringWriting(10));
    }

    @Test
    void test_shouldBeCompacted_yes() {
        when(segmentPropertiesManager.getSegmentStats())
                .thenReturn(new SegmentStats(31, 1000L, 15));
        when(segmentConf.getMaxNumberOfKeysInDeltaCache()).thenReturn(30L);

        assertTrue(sc.shouldBeCompacted());
    }

    @Test
    void test_shouldBeCompacted_no() {
        when(segmentPropertiesManager.getSegmentStats())
                .thenReturn(new SegmentStats(31, 1000L, 15));
        when(segmentConf.getMaxNumberOfKeysInDeltaCache()).thenReturn(35L);

        assertFalse(sc.shouldBeCompacted());
    }

}
