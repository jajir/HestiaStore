package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segment.Segment;
import org.junit.jupiter.api.Test;

class SegmentSplitCoordinatorTest {

    @Test
    void shouldBeSplit_reflectsCacheThreshold() {
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = (Segment<Integer, String>) mock(
                Segment.class);
        when(segment.getNumberOfKeysInCache()).thenReturn(9L, 10L);

        final SegmentSplitCoordinator<Integer, String> coordinator = new SegmentSplitCoordinator<>(
                IndexConfiguration.<Integer, String>builder().build(),
                mock(KeySegmentCache.class), mock(SegmentRegistry.class));

        assertFalse(coordinator.shouldBeSplit(segment, 10));
        assertTrue(coordinator.shouldBeSplit(segment, 10));
    }

    @Test
    void optionallySplit_shortCircuitsWhenBelowThreshold() {
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = (Segment<Integer, String>) mock(
                Segment.class);
        when(segment.getNumberOfKeysInCache()).thenReturn(1L);

        final KeySegmentCache<Integer> keySegmentCache = mock(KeySegmentCache.class);
        final SegmentRegistry<Integer, String> segmentRegistry = mock(
                SegmentRegistry.class);
        final SegmentSplitCoordinator<Integer, String> coordinator = new SegmentSplitCoordinator<>(
                IndexConfiguration.<Integer, String>builder().build(),
                keySegmentCache, segmentRegistry);

        assertFalse(coordinator.optionallySplit(segment, 100L));
        verifyNoInteractions(segmentRegistry);
    }
}
