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
            policy = new SegmentMaintenancePolicyThreshold<>(10, 5);
        }

        @Test
        void flush_only_when_write_cache_threshold_reached() {
            when(segment.getNumberOfKeysInWriteCache()).thenReturn(5);
            when(segment.getNumberOfKeysInSegmentCache()).thenReturn(4L);

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
    class DisabledThresholds {

        private SegmentMaintenancePolicyThreshold<Integer, String> policy;

        @BeforeEach
        void setUp() {
            policy = new SegmentMaintenancePolicyThreshold<>(0, 0);
        }

        @Test
        void none_when_thresholds_disabled_even_if_counts_high() {
            final SegmentMaintenanceDecision decision = policy
                    .evaluateAfterWrite(segment);

            assertFalse(decision.shouldFlush());
            assertFalse(decision.shouldCompact());
        }
    }

    @Nested
    class WriteCacheOnly {

        private SegmentMaintenancePolicyThreshold<Integer, String> policy;

        @BeforeEach
        void setUp() {
            policy = new SegmentMaintenancePolicyThreshold<>(0, 5);
        }

        @Test
        void flush_when_write_cache_reached_and_segment_cache_disabled() {
            when(segment.getNumberOfKeysInWriteCache()).thenReturn(5);

            final SegmentMaintenanceDecision decision = policy
                    .evaluateAfterWrite(segment);

            assertTrue(decision.shouldFlush());
            assertFalse(decision.shouldCompact());
        }
    }

    @Nested
    class SegmentCacheOnly {

        private SegmentMaintenancePolicyThreshold<Integer, String> policy;

        @BeforeEach
        void setUp() {
            policy = new SegmentMaintenancePolicyThreshold<>(10, 0);
        }

        @Test
        void none_when_write_cache_disabled_and_segment_cache_below() {
            when(segment.getNumberOfKeysInSegmentCache()).thenReturn(9L);

            final SegmentMaintenanceDecision decision = policy
                    .evaluateAfterWrite(segment);

            assertFalse(decision.shouldFlush());
            assertFalse(decision.shouldCompact());
        }

        @Test
        void compact_when_segment_cache_reached_and_write_cache_disabled() {
            when(segment.getNumberOfKeysInSegmentCache()).thenReturn(10L);

            final SegmentMaintenanceDecision decision = policy
                    .evaluateAfterWrite(segment);

            assertFalse(decision.shouldFlush());
            assertTrue(decision.shouldCompact());
        }
    }

    @Test
    void constructor_rejects_negative_thresholds() {
        final IllegalArgumentException segmentCache = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentMaintenancePolicyThreshold<>(-1, 1));
        assertTrue(segmentCache.getMessage().contains("maxSegmentCacheKeys"));

        final IllegalArgumentException writeCache = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentMaintenancePolicyThreshold<>(1, -1));
        assertTrue(writeCache.getMessage().contains("maxWriteCacheKeys"));
    }
}
