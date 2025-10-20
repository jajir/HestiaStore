package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentReplacerTest {

    @Mock
    private SegmentFilesRenamer filesRenamer;

    @Mock
    private SegmentDeltaCacheController<String, String> deltaCacheController;

    @Mock
    private SegmentPropertiesManager segmentPropertiesManager;

    @Mock
    private SegmentFiles<String, String> targetSegmentFiles;

    @Mock
    private SegmentImpl<String, String> lowerSegment;

    @Mock
    private SegmentPropertiesManager lowerSegmentProps;

    @Mock
    private SegmentFiles<String, String> lowerSegmentFiles;

    @Mock
    private SegmentStats lowerStats;

    @Test
    void replaceWithLower_applies_changes_and_updates_stats() {
        when(lowerSegment.getSegmentFiles()).thenReturn(lowerSegmentFiles);
        when(lowerSegment.getSegmentPropertiesManager()).thenReturn(lowerSegmentProps);
        when(lowerSegmentProps.getSegmentStats()).thenReturn(lowerStats);
        when(lowerStats.getNumberOfKeysInSegment()).thenReturn(42L);
        when(lowerStats.getNumberOfKeysInScarceIndex()).thenReturn(7L);

        final SegmentReplacer<String, String> replacer = new SegmentReplacer<>(
                filesRenamer, deltaCacheController, segmentPropertiesManager,
                targetSegmentFiles);

        replacer.replaceWithLower(lowerSegment);

        verify(filesRenamer).renameFiles(lowerSegmentFiles, targetSegmentFiles);
        verify(deltaCacheController).clear();
        verify(segmentPropertiesManager).setNumberOfKeysInCache(0);
        verify(segmentPropertiesManager).setNumberOfKeysInIndex(42L);
        verify(segmentPropertiesManager).setNumberOfKeysInScarceIndex(7);
    }

    @Test
    void replaceWithLower_null_throws() {
        final SegmentReplacer<String, String> replacer = new SegmentReplacer<>(
                filesRenamer, deltaCacheController, segmentPropertiesManager,
                targetSegmentFiles);

        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> replacer.replaceWithLower(null));
        assertEquals("Property 'lowerSegment' must not be null.",
                err.getMessage());
    }
}
