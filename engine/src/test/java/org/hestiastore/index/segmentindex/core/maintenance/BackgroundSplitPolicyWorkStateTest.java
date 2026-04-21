package org.hestiastore.index.segmentindex.core.maintenance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class BackgroundSplitPolicyWorkStateTest {

    @Test
    void consumeHintedSegmentIdsClearsPendingHints() {
        final BackgroundSplitPolicyWorkState workState =
                new BackgroundSplitPolicyWorkState();
        final SegmentId firstSegmentId = SegmentId.of(1);
        final SegmentId secondSegmentId = SegmentId.of(2);
        workState.addHint(firstSegmentId);
        workState.addHint(secondSegmentId);

        final List<SegmentId> hintedSegmentIds = workState
                .consumeHintedSegmentIds();

        assertEquals(2, hintedSegmentIds.size());
        assertTrue(hintedSegmentIds.contains(firstSegmentId));
        assertTrue(hintedSegmentIds.contains(secondSegmentId));
        assertFalse(workState.hasPendingHints());
    }

    @Test
    void clearPendingWorkResetsRequestedScanAndHints() {
        final BackgroundSplitPolicyWorkState workState =
                new BackgroundSplitPolicyWorkState();
        workState.markScanRequested();
        workState.addHint(SegmentId.of(9));

        workState.clearPendingWork();

        assertFalse(workState.isScanRequested());
        assertFalse(workState.hasPendingHints());
        assertFalse(workState.hasPendingWork());
    }
}
