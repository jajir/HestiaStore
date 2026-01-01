package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SegmentSplitterPolicyTest {

    private static final long MAX_KEYS_IN_SEGMENT = 100L;

    @Test
    void shouldRequestCompactionForLowNumberOfKeys() {
        final SegmentSplitterPolicy<String, String> policy = new SegmentSplitterPolicy<>(
                2L, false);
        assertTrue(policy.shouldBeCompactedBeforeSplitting(MAX_KEYS_IN_SEGMENT));
    }

    @Test
    void shouldDecideBasedOnEstimatedKeys() {
        final SegmentSplitterPolicy<String, String> policyBelow = new SegmentSplitterPolicy<>(
                80L, false);
        assertTrue(policyBelow
                .shouldBeCompactedBeforeSplitting(MAX_KEYS_IN_SEGMENT));

        final SegmentSplitterPolicy<String, String> policyAbove = new SegmentSplitterPolicy<>(
                95L, false);
        assertFalse(policyAbove
                .shouldBeCompactedBeforeSplitting(MAX_KEYS_IN_SEGMENT));
    }

    @Test
    void estimateNumberOfKeysCombinesIndexAndCache() {
        final SegmentSplitterPolicy<String, String> policy = new SegmentSplitterPolicy<>(
                57L, true);
        assertEquals(57L, policy.estimateNumberOfKeys());
        assertTrue(policy.hasTombstonesInDeltaCache());
    }

    @Test
    void rejects_negative_estimated_keys() {
        final IllegalArgumentException err = org.junit.jupiter.api.Assertions
                .assertThrows(IllegalArgumentException.class,
                        () -> new SegmentSplitterPolicy<>(-1L, false));
        assertEquals(
                "Property 'estimatedNumberOfKeys' must be >= 0.",
                err.getMessage());
    }
}
