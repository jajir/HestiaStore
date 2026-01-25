package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentMaintenancePolicyThresholdTest {

    @Mock
    private Segment<Integer, String> segment;

    @Nested
    class DefaultThresholds {

        private SegmentMaintenancePolicyThreshold<Integer, String> policy;

        @BeforeEach
        void setUp() {
            policy = new SegmentMaintenancePolicyThreshold<>(10, 5, 3);
        }

        @Test
        void flush_only_when_write_cache_threshold_reached() {
            when(segment.getNumberOfKeysInWriteCache()).thenReturn(5);
            when(segment.getNumberOfKeysInSegmentCache()).thenReturn(4L);
            when(segment.getNumberOfDeltaCacheFiles()).thenReturn(3);

            final SegmentMaintenanceDecision decision = policy
                    .evaluateAfterWrite(segment);

            assertTrue(decision.shouldFlush());
            assertFalse(decision.shouldCompact());
        }

        @Test
        void compact_only_when_segment_cache_threshold_reached() {
            when(segment.getNumberOfKeysInSegmentCache()).thenReturn(10L);

            final SegmentMaintenanceDecision decision = policy
                    .evaluateAfterWrite(segment);

            assertFalse(decision.shouldFlush());
            assertTrue(decision.shouldCompact());
        }

        @Test
        void compact_takes_priority_when_both_thresholds_reached() {
            when(segment.getNumberOfKeysInSegmentCache()).thenReturn(12L);

            final SegmentMaintenanceDecision decision = policy
                    .evaluateAfterWrite(segment);

            assertFalse(decision.shouldFlush());
            assertTrue(decision.shouldCompact());
        }

        @Test
        void none_when_thresholds_not_reached() {
            when(segment.getNumberOfKeysInWriteCache()).thenReturn(1);
            when(segment.getNumberOfKeysInSegmentCache()).thenReturn(2L);

            final SegmentMaintenanceDecision decision = policy
                    .evaluateAfterWrite(segment);

            assertFalse(decision.shouldFlush());
            assertFalse(decision.shouldCompact());
        }
    }

    @Nested
    class ZeroSegmentCacheThreshold {

        private SegmentMaintenancePolicyThreshold<Integer, String> policy;

        @BeforeEach
        void setUp() {
            policy = new SegmentMaintenancePolicyThreshold<>(0, 5, 3);
        }

        @Test
        void compact_when_segment_cache_threshold_is_zero() {
            when(segment.getNumberOfKeysInSegmentCache()).thenReturn(0L);
            final SegmentMaintenanceDecision decision = policy
                    .evaluateAfterWrite(segment);

            assertFalse(decision.shouldFlush());
            assertTrue(decision.shouldCompact());
        }
    }

    @Nested
    class WriteCacheThreshold {

        private SegmentMaintenancePolicyThreshold<Integer, String> policy;

        @BeforeEach
        void setUp() {
            policy = new SegmentMaintenancePolicyThreshold<>(10, 5, 3);
        }

        @Test
        void flush_when_write_cache_reached_and_segment_cache_below_threshold() {
            when(segment.getNumberOfKeysInSegmentCache()).thenReturn(9L);
            when(segment.getNumberOfKeysInWriteCache()).thenReturn(5);
            when(segment.getNumberOfDeltaCacheFiles()).thenReturn(0);

            final SegmentMaintenanceDecision decision = policy
                    .evaluateAfterWrite(segment);

            assertTrue(decision.shouldFlush());
            assertFalse(decision.shouldCompact());
        }
    }

    @Nested
    class SegmentCacheThreshold {

        private SegmentMaintenancePolicyThreshold<Integer, String> policy;

        @BeforeEach
        void setUp() {
            policy = new SegmentMaintenancePolicyThreshold<>(10, 5, 3);
        }

        @Test
        void none_when_segment_cache_below_and_write_cache_below() {
            when(segment.getNumberOfKeysInSegmentCache()).thenReturn(9L);
            when(segment.getNumberOfKeysInWriteCache()).thenReturn(4);

            final SegmentMaintenanceDecision decision = policy
                    .evaluateAfterWrite(segment);

            assertFalse(decision.shouldFlush());
            assertFalse(decision.shouldCompact());
        }

        @Test
        void compact_when_segment_cache_reached() {
            when(segment.getNumberOfKeysInSegmentCache()).thenReturn(10L);

            final SegmentMaintenanceDecision decision = policy
                    .evaluateAfterWrite(segment);

            assertFalse(decision.shouldFlush());
            assertTrue(decision.shouldCompact());
        }
    }

    @Nested
    class DeltaCacheFilesWithWriteCache {

        private SegmentMaintenancePolicyThreshold<Integer, String> policy;

        @BeforeEach
        void setUp() {
            policy = new SegmentMaintenancePolicyThreshold<>(10, 5, 3);
        }

        @Test
        void compact_when_write_cache_reached_and_delta_cache_exceeds_cap() {
            when(segment.getNumberOfKeysInSegmentCache()).thenReturn(4L);
            when(segment.getNumberOfKeysInWriteCache()).thenReturn(5);
            when(segment.getNumberOfDeltaCacheFiles()).thenReturn(4);

            final SegmentMaintenanceDecision decision = policy
                    .evaluateAfterWrite(segment);

            assertFalse(decision.shouldFlush());
            assertTrue(decision.shouldCompact());
        }

        @Test
        void flush_when_write_cache_reached_and_delta_cache_at_cap() {
            when(segment.getNumberOfKeysInSegmentCache()).thenReturn(4L);
            when(segment.getNumberOfKeysInWriteCache()).thenReturn(5);
            when(segment.getNumberOfDeltaCacheFiles()).thenReturn(3);

            final SegmentMaintenanceDecision decision = policy
                    .evaluateAfterWrite(segment);

            assertTrue(decision.shouldFlush());
            assertFalse(decision.shouldCompact());
        }

        @Test
        void none_when_write_cache_below_threshold_even_if_delta_cache_exceeds() {
            when(segment.getNumberOfKeysInSegmentCache()).thenReturn(4L);
            when(segment.getNumberOfKeysInWriteCache()).thenReturn(4);

            final SegmentMaintenanceDecision decision = policy
                    .evaluateAfterWrite(segment);

            assertFalse(decision.shouldFlush());
            assertFalse(decision.shouldCompact());
        }
    }

    @Test
    void constructor_rejects_negative_thresholds() {
        final IllegalArgumentException segmentCache = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentMaintenancePolicyThreshold<>(-1, 1, 0));
        assertTrue(segmentCache.getMessage().contains("maxSegmentCacheKeys"));

        final IllegalArgumentException writeCache = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentMaintenancePolicyThreshold<>(1, -1, 0));
        assertTrue(writeCache.getMessage().contains("maxWriteCacheKeys"));

        final IllegalArgumentException deltaCache = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentMaintenancePolicyThreshold<>(1, 1, -1));
        assertTrue(deltaCache.getMessage().contains("maxDeltaCacheFiles"));
    }
}
