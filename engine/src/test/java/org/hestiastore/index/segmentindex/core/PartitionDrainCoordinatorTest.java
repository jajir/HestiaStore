package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.partition.PartitionRuntime;
import org.hestiastore.index.segmentindex.partition.PartitionRuntimeLimits;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PartitionDrainCoordinatorTest {

    @Mock
    private StableSegmentCoordinator<Integer, String> stableSegmentCoordinator;

    private Directory directory;
    private KeyToSegmentMap<Integer> keyToSegmentMap;
    private KeyToSegmentMapSynchronizedAdapter<Integer> synchronizedKeyToSegmentMap;
    private PartitionRuntime<Integer, String> partitionRuntime;
    private PartitionRuntimeLimits limits;
    private AtomicInteger splitHints;
    private AtomicReference<RuntimeException> handledFailure;
    private PartitionDrainCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        keyToSegmentMap = new KeyToSegmentMap<>(directory,
                new TypeDescriptorInteger());
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
        partitionRuntime = new PartitionRuntime<>(Integer::compareTo);
        limits = new PartitionRuntimeLimits(4, 2, 8, 16);
        splitHints = new AtomicInteger(0);
        handledFailure = new AtomicReference<>();
        coordinator = new PartitionDrainCoordinator<>(partitionRuntime,
                synchronizedKeyToSegmentMap, Runnable::run,
                new IndexRetryPolicy(1, 10), stableSegmentCoordinator,
                new Stats(), ignoredSegmentId -> splitHints.incrementAndGet(),
                handledFailure::set);
    }

    @AfterEach
    void tearDown() {
        if (synchronizedKeyToSegmentMap != null
                && !synchronizedKeyToSegmentMap.wasClosed()) {
            synchronizedKeyToSegmentMap.close();
        }
    }

    @Test
    void drainPartitions_waitsForRunCompletionAndFlushesStableSegment() {
        final SegmentId segmentId = keyToSegmentMap.insertKeyToSegment(1);
        partitionRuntime.write(segmentId, 1, "v1", limits);
        partitionRuntime.write(segmentId, 2, "v2", limits);

        coordinator.drainPartitions(true);

        verify(stableSegmentCoordinator).putEntryForDrain(segmentId, 1, "v1");
        verify(stableSegmentCoordinator).putEntryForDrain(segmentId, 2, "v2");
        verify(stableSegmentCoordinator).flushSegment(segmentId, true);
        assertEquals(0, partitionRuntime.snapshot().getBufferedKeyCount());
        assertEquals(1, splitHints.get());
        assertNull(handledFailure.get());
    }

    @Test
    void scheduleDrain_skipsSegmentsWithoutPendingRun() {
        final SegmentId segmentId = keyToSegmentMap.insertKeyToSegment(1);

        coordinator.scheduleDrain(segmentId);

        assertEquals(0, splitHints.get());
        assertEquals(0, partitionRuntime.snapshot().getDrainInFlightCount());
        verifyNoInteractions(stableSegmentCoordinator);
    }

    @Test
    void scheduleDrain_recordsTaskStartDelayAndRunLatency() {
        final SegmentId segmentId = keyToSegmentMap.insertKeyToSegment(1);
        final AtomicReference<Runnable> scheduledTask = new AtomicReference<>();
        final AtomicLong nowNanos = new AtomicLong(
                TimeUnit.MILLISECONDS.toNanos(2));
        final Stats stats = new Stats();
        partitionRuntime.write(segmentId, 1, "v1", limits);
        partitionRuntime.sealAllActivePartitionsForDrain();
        doAnswer(invocation -> {
            nowNanos.set(TimeUnit.MILLISECONDS.toNanos(9));
            return null;
        }).when(stableSegmentCoordinator).flushSegment(segmentId, true);
        coordinator = new PartitionDrainCoordinator<>(partitionRuntime,
                synchronizedKeyToSegmentMap, scheduledTask::set,
                new IndexRetryPolicy(1, 10), stableSegmentCoordinator, stats,
                ignoredSegmentId -> splitHints.incrementAndGet(),
                handledFailure::set, nowNanos::get);

        coordinator.scheduleDrain(segmentId);
        nowNanos.set(TimeUnit.MILLISECONDS.toNanos(5));

        scheduledTask.get().run();

        assertEquals(3_000L, stats.getDrainTaskStartDelayP95Micros());
        assertEquals(4_000L, stats.getDrainTaskRunLatencyP95Micros());
        verify(stableSegmentCoordinator).flushSegment(segmentId, true);
    }
}
