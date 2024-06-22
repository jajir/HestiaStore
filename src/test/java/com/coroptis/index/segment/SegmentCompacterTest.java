package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SegmentCompacterTest {

    @Mock
    private SegmentFiles<Integer, String> segmentFiles;

    @Mock
    private SegmentConf segmentConf;

    @Mock
    private VersionController versionController;

    @Mock
    private SegmentPropertiesManager segmentPropertiesManager;

    @Mock
    private SegmentCacheDataProvider<Integer, String> segmentCacheDataProvider;

    @Test
    public void test_basic_operations() throws Exception {
        final SegmentCompacter<Integer, String> sc = new SegmentCompacter<>(
                segmentFiles, segmentConf, versionController,
                segmentPropertiesManager, segmentCacheDataProvider);

        assertNotNull(sc);
    }

    @Test
    public void test_shouldBeCompacted() throws Exception {
        final SegmentCompacter<Integer, String> sc = new SegmentCompacter<>(
                segmentFiles, segmentConf, versionController,
                segmentPropertiesManager, segmentCacheDataProvider);
        when(segmentPropertiesManager.getSegmentStats())
                .thenReturn(new SegmentStats(10, 1000L, 15));
        when(segmentConf.getMaxNumberOfKeysInSegmentCache()).thenReturn(30L,
                20L);

        assertFalse(sc.shouldBeCompacted(10));
        assertTrue(sc.shouldBeCompacted(25));

        verify(segmentConf, never()).getMaxNumberOfKeysInSegmentMemory();
    }

    @Test
    public void test_shouldBeCompactedDuringWriting() throws Exception {
        final SegmentCompacter<Integer, String> sc = new SegmentCompacter<>(
                segmentFiles, segmentConf, versionController,
                segmentPropertiesManager, segmentCacheDataProvider);
        when(segmentPropertiesManager.getSegmentStats())
                .thenReturn(new SegmentStats(10, 1000L, 15));
        when(segmentConf.getMaxNumberOfKeysInSegmentMemory()).thenReturn(30L,
                20L);

        assertFalse(sc.shouldBeCompactedDuringWriting(10));
        assertTrue(sc.shouldBeCompactedDuringWriting(25));

        verify(segmentConf, never()).getMaxNumberOfKeysInSegmentCache();
    }

}
