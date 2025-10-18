package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentCompactionPolicyTest {

    private SegmentCompactionPolicy policy;

    @BeforeEach
    void setUp() {
        final SegmentConf segmentConf = new SegmentConf(10L, 25L, 3, null, null,
                null, 1024, List.of(new ChunkFilterDoNothing()),
                List.of(new ChunkFilterDoNothing()));
        policy = new SegmentCompactionPolicy(segmentConf);
    }

    @Test
    void shouldCompact_returnsTrueWhenDeltaAboveLimit() {
        final SegmentStats stats = new SegmentStats(11, 0, 0);

        assertTrue(policy.shouldCompact(stats));
    }

    @Test
    void shouldCompact_returnsFalseWhenDeltaWithinLimit() {
        final SegmentStats stats = new SegmentStats(10, 0, 0);

        assertFalse(policy.shouldCompact(stats));
    }

    @Test
    void shouldCompactDuringWriting_returnsTrueWhenThresholdExceeded() {
        final SegmentStats stats = new SegmentStats(20, 0, 0);

        assertTrue(policy.shouldCompactDuringWriting(6, stats));
    }

    @Test
    void shouldCompactDuringWriting_returnsFalseWhenThresholdNotReached() {
        final SegmentStats stats = new SegmentStats(20, 0, 0);

        assertFalse(policy.shouldCompactDuringWriting(5, stats));
    }

    @Test
    void shouldCompactDuringWriting_throwsWhenNegativeKeysProvided() {
        final SegmentStats stats = new SegmentStats(0, 0, 0);

        assertThrows(IllegalArgumentException.class,
                () -> policy.shouldCompactDuringWriting(-1, stats));
    }
}
