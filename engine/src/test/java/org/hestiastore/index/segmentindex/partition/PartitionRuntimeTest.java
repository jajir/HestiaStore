package org.hestiastore.index.segmentindex.partition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.NavigableMap;
import java.util.TreeMap;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.split.SegmentSplitApplyPlan;
import org.hestiastore.index.segmentindex.split.SegmentSplitterResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PartitionRuntimeTest {

    private PartitionRuntime<Integer, String> runtime;

    @BeforeEach
    void setUp() {
        runtime = new PartitionRuntime<>(Integer::compareTo);
    }

    @Test
    void writeStoresValueInActivePartitionAndLookupSeesItImmediately() {
        final SegmentId segmentId = SegmentId.of(1);
        final PartitionRuntimeLimits limits = new PartitionRuntimeLimits(4, 2,
                8, 16);

        final PartitionWriteResult result = runtime.write(segmentId,
                Integer.valueOf(11), "value-11", limits);

        assertEquals(PartitionWriteResultStatus.OK, result.getStatus());
        assertTrue(runtime.lookup(segmentId, Integer.valueOf(11)).isFound());
        assertEquals("value-11",
                runtime.lookup(segmentId, Integer.valueOf(11)).getValue());
    }

    @Test
    void lookupPrefersNewestOverlayValueOverOlderImmutableRun() {
        final SegmentId segmentId = SegmentId.of(2);
        final PartitionRuntimeLimits limits = new PartitionRuntimeLimits(1, 2,
                8, 16);

        assertEquals(PartitionWriteResultStatus.OK,
                runtime.write(segmentId, Integer.valueOf(7), "old", limits)
                        .getStatus());
        assertEquals(PartitionWriteResultStatus.OK,
                runtime.write(segmentId, Integer.valueOf(7), "new", limits)
                        .getStatus());

        final NavigableMap<Integer, String> snapshot = new TreeMap<>();
        snapshot.put(Integer.valueOf(7), "stable");
        runtime.applyOverlaySnapshot(java.util.List.of(segmentId), snapshot);

        assertEquals("new", snapshot.get(Integer.valueOf(7)));
        assertEquals("new",
                runtime.lookup(segmentId, Integer.valueOf(7)).getValue());
    }

    @Test
    void writeReturnsBusyWhenPartitionBufferIsFull() {
        final SegmentId segmentId = SegmentId.of(3);
        final PartitionRuntimeLimits limits = new PartitionRuntimeLimits(1, 1,
                2, 8);

        assertEquals(PartitionWriteResultStatus.OK,
                runtime.write(segmentId, Integer.valueOf(1), "v1", limits)
                        .getStatus());
        assertEquals(PartitionWriteResultStatus.OK,
                runtime.write(segmentId, Integer.valueOf(2), "v2", limits)
                        .getStatus());

        final PartitionWriteResult third = runtime.write(segmentId,
                Integer.valueOf(3), "v3", limits);

        assertEquals(PartitionWriteResultStatus.BUSY, third.getStatus());
        assertEquals(2, runtime.snapshot().getBufferedKeyCount());
        assertTrue(runtime.snapshot().getLocalThrottleCount() > 0L);
    }

    @Test
    void sealAndDrainRemoveBufferedRunFromSnapshot() {
        final SegmentId segmentId = SegmentId.of(4);
        final PartitionRuntimeLimits limits = new PartitionRuntimeLimits(4, 2,
                8, 16);

        runtime.write(segmentId, Integer.valueOf(1), "v1", limits);
        runtime.write(segmentId, Integer.valueOf(2), "v2", limits);
        runtime.sealAllActivePartitionsForDrain();

        assertTrue(runtime.markDrainScheduledIfNeeded(segmentId));
        final PartitionImmutableRun<Integer, String> run = runtime
                .peekOldestImmutableRun(segmentId);
        assertEquals(2, run.size());

        runtime.completeDrainedRun(segmentId, run);
        runtime.finishDrainScheduling(segmentId);

        assertFalse(runtime.hasBufferedData());
        assertEquals(0, runtime.snapshot().getBufferedKeyCount());
        assertEquals(0, runtime.snapshot().getImmutableRunCount());
    }

    @Test
    void reassignOverlayAfterSplitMovesBufferedEntriesToChildPartitions() {
        final SegmentId oldSegmentId = SegmentId.of(5);
        final SegmentId lowerSegmentId = SegmentId.of(6);
        final SegmentId upperSegmentId = SegmentId.of(7);
        final PartitionRuntimeLimits limits = new PartitionRuntimeLimits(2, 2,
                8, 16);

        runtime.write(oldSegmentId, Integer.valueOf(1), "v1", limits);
        runtime.write(oldSegmentId, Integer.valueOf(9), "v9", limits);
        runtime.write(oldSegmentId, Integer.valueOf(2), "v2", limits);
        runtime.write(oldSegmentId, Integer.valueOf(9), "v9-new", limits);

        runtime.reassignOverlayAfterSplit(new SegmentSplitApplyPlan<>(
                oldSegmentId, lowerSegmentId, upperSegmentId,
                Integer.valueOf(1), Integer.valueOf(2),
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT));

        assertFalse(runtime.lookup(oldSegmentId, Integer.valueOf(1)).isFound());
        assertEquals("v1",
                runtime.lookup(lowerSegmentId, Integer.valueOf(1)).getValue());
        assertEquals("v2",
                runtime.lookup(lowerSegmentId, Integer.valueOf(2)).getValue());
        assertEquals("v9-new", runtime.lookup(upperSegmentId,
                Integer.valueOf(9)).getValue());
        assertEquals(3, runtime.snapshot().getBufferedKeyCount());
    }

    @Test
    void splitFreezeBlocksDrainSchedulingUntilFinished() {
        final SegmentId segmentId = SegmentId.of(8);
        final PartitionRuntimeLimits limits = new PartitionRuntimeLimits(1, 2,
                8, 16);

        runtime.write(segmentId, Integer.valueOf(1), "v1", limits);
        runtime.write(segmentId, Integer.valueOf(2), "v2", limits);

        runtime.beginSplit(segmentId);
        assertFalse(runtime.markDrainScheduledIfNeeded(segmentId));

        runtime.finishSplit(segmentId);
        assertTrue(runtime.markDrainScheduledIfNeeded(segmentId));
    }

    @Test
    void finishingDrainAfterParentRouteWasRemovedStillClearsInFlightCount() {
        final SegmentId oldSegmentId = SegmentId.of(12);
        final SegmentId lowerSegmentId = SegmentId.of(13);
        final SegmentId upperSegmentId = SegmentId.of(14);
        final PartitionRuntimeLimits limits = new PartitionRuntimeLimits(1, 2,
                8, 16);

        runtime.write(oldSegmentId, Integer.valueOf(1), "v1", limits);
        runtime.write(oldSegmentId, Integer.valueOf(2), "v2", limits);
        runtime.sealAllActivePartitionsForDrain();
        assertTrue(runtime.markDrainScheduledIfNeeded(oldSegmentId));
        assertEquals(1, runtime.snapshot().getDrainInFlightCount());

        runtime.reassignOverlayAfterSplit(new SegmentSplitApplyPlan<>(
                oldSegmentId, lowerSegmentId, upperSegmentId,
                Integer.valueOf(1), Integer.valueOf(1),
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT));
        runtime.finishDrainScheduling(oldSegmentId);

        assertEquals(0, runtime.snapshot().getDrainInFlightCount());
    }
}
