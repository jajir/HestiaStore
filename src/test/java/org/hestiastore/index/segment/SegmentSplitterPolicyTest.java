package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitterPolicyTest {

    private static final long MAX_KEYS_IN_SEGMENT = 100L;

    @Mock
    private SegmentPropertiesManager segmentPropertiesManager;

    @Mock
    private SegmentDeltaCacheController<String, String> deltaCacheController;

    private SegmentSplitterPolicy<String, String> policy;

    @BeforeEach
    void setUp() {
        policy = new SegmentSplitterPolicy<>(segmentPropertiesManager,
                deltaCacheController);
    }

    @Test
    void shouldRequestCompactionForLowNumberOfKeys() {
        final SegmentStats stats = new SegmentStats(0L, 2L, 0L);
        when(segmentPropertiesManager.getSegmentStats()).thenReturn(stats);
        when(deltaCacheController.getDeltaCacheSizeWithoutTombstones())
                .thenReturn(0);

        assertTrue(
                policy.shouldBeCompactedBeforeSplitting(MAX_KEYS_IN_SEGMENT));
    }

    @Test
    void shouldDecideBasedOnEstimatedKeys() {
        final SegmentStats stats = new SegmentStats(0L, 80L, 0L);
        when(segmentPropertiesManager.getSegmentStats()).thenReturn(stats);
        when(deltaCacheController.getDeltaCacheSizeWithoutTombstones())
                .thenReturn(15);

        assertFalse(
                policy.shouldBeCompactedBeforeSplitting(MAX_KEYS_IN_SEGMENT));

        when(deltaCacheController.getDeltaCacheSizeWithoutTombstones())
                .thenReturn(0);

        assertTrue(
                policy.shouldBeCompactedBeforeSplitting(MAX_KEYS_IN_SEGMENT));
    }

    @Test
    void estimateNumberOfKeysCombinesIndexAndCache() {
        final SegmentStats stats = new SegmentStats(0L, 50L, 0L);
        when(segmentPropertiesManager.getSegmentStats()).thenReturn(stats);
        when(deltaCacheController.getDeltaCacheSizeWithoutTombstones())
                .thenReturn(7);

        assertEquals(57L, policy.estimateNumberOfKeys());
    }
}
