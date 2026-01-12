package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segment.Segment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitCoordinatorTest {

    @Mock
    private Segment<Integer, String> segment;

    @Mock
    private KeyToSegmentMap<Integer> keyToSegmentMap;

    private KeyToSegmentMapSynchronizedAdapter<Integer> synchronizedKeyToSegmentMap;

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    private SegmentSplitCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
        coordinator = new SegmentSplitCoordinator<>(
                IndexConfiguration.<Integer, String>builder().build(),
                synchronizedKeyToSegmentMap, segmentRegistry);
    }

    @AfterEach
    void tearDown() {
        coordinator = null;
        synchronizedKeyToSegmentMap = null;
    }

    @Test
    void shouldBeSplit_reflectsCacheThreshold() {
        when(segment.getNumberOfKeysInCache()).thenReturn(9L, 10L);

        assertFalse(coordinator.shouldBeSplit(segment, 10));
        assertTrue(coordinator.shouldBeSplit(segment, 10));
    }

    @Test
    void optionallySplit_shortCircuitsWhenBelowThreshold() {
        when(segment.getNumberOfKeysInCache()).thenReturn(1L);

        assertFalse(coordinator.optionallySplit(segment, 100L));
        verifyNoInteractions(segmentRegistry);
    }
}
