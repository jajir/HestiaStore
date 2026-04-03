package org.hestiastore.index.segmentindex.core;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentindex.partition.PartitionRuntime;
import org.hestiastore.index.segmentindex.split.PartitionStableSplitCoordinator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BackgroundSplitCoordinatorTest {

    @Mock
    private KeyToSegmentMap<String> keyToSegmentMap;

    private KeyToSegmentMapSynchronizedAdapter<String> synchronizedKeyToSegmentMap;

    @Mock
    private Segment<String, String> segment;

    private PartitionRuntime<String, String> partitionRuntime;

    @Mock
    private PartitionStableSplitCoordinator<String, String> splitCoordinator;

    @BeforeEach
    void setUp() {
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
        partitionRuntime = new PartitionRuntime<>(String::compareTo);
    }

    @Test
    void returnsEarlyWhenSegmentIsClosed() {
        when(segment.getState()).thenReturn(SegmentState.CLOSED);

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinator<>(
                synchronizedKeyToSegmentMap, partitionRuntime,
                splitCoordinator, Runnable::run, failure -> {
                }, () -> {
                });

        coordinator.handleSplitCandidate(segment, 100L);

        verifyNoInteractions(keyToSegmentMap, splitCoordinator);
        verify(segment, never()).getNumberOfKeysInCache();
    }

    @Test
    void usesSegmentSizeThreshold_notSegmentCacheThreshold() {
        final SegmentId segmentId = SegmentId.of(1);
        when(segment.getId()).thenReturn(segmentId);
        when(segment.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(segment.getNumberOfKeysInCache()).thenReturn(50L);

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinator<>(
                synchronizedKeyToSegmentMap, partitionRuntime,
                splitCoordinator, Runnable::run, failure -> {
                }, () -> {
                });

        coordinator.handleSplitCandidate(segment, 1000L);

        verify(segment).getNumberOfKeysInCache();
        verifyNoInteractions(splitCoordinator);
    }

    @Test
    void schedulesSplitAfterDrainForStillMappedSegment() {
        final SegmentId segmentId = SegmentId.of(1);
        when(segment.getId()).thenReturn(segmentId);
        when(segment.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(segment.getNumberOfKeysInCache()).thenReturn(101L);

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinator<>(
                synchronizedKeyToSegmentMap, partitionRuntime,
                splitCoordinator, Runnable::run, failure -> {
                }, () -> {
                });

        coordinator.handleSplitCandidate(segment, 100L);

        verify(splitCoordinator).optionallySplit(eq(segment), eq(100L), any());
    }

    @Test
    void schedulesSplitOnDedicatedExecutor() {
        final SegmentId segmentId = SegmentId.of(1);
        final AtomicReference<Runnable> scheduledTask = new AtomicReference<>();
        final Executor splitExecutor = scheduledTask::set;
        when(segment.getId()).thenReturn(segmentId);
        when(segment.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(segment.getNumberOfKeysInCache()).thenReturn(101L);

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinator<>(
                synchronizedKeyToSegmentMap, partitionRuntime,
                splitCoordinator, splitExecutor, failure -> {
                }, () -> {
                });

        coordinator.handleSplitCandidate(segment, 100L);

        verifyNoInteractions(splitCoordinator);
        final Runnable task = scheduledTask.get();
        org.junit.jupiter.api.Assertions.assertNotNull(task);
        org.junit.jupiter.api.Assertions.assertEquals(1,
                coordinator.splitInFlightCount());

        task.run();

        verify(splitCoordinator).optionallySplit(eq(segment), eq(100L), any());
        coordinator.awaitSplitsIdle(1_000L);
        org.junit.jupiter.api.Assertions.assertEquals(0,
                coordinator.splitInFlightCount());
    }

    @Test
    void recordsSplitTaskStartDelayAndRunLatency() {
        final SegmentId segmentId = SegmentId.of(6);
        final AtomicReference<Runnable> scheduledTask = new AtomicReference<>();
        final AtomicLong nowNanos = new AtomicLong(TimeUnit.MILLISECONDS
                .toNanos(2));
        final Stats stats = new Stats();
        when(segment.getId()).thenReturn(segmentId);
        when(segment.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(segment.getNumberOfKeysInCache()).thenReturn(101L);
        when(splitCoordinator.optionallySplit(eq(segment), eq(100L), any()))
                .thenAnswer(invocation -> {
                    nowNanos.set(TimeUnit.MILLISECONDS.toNanos(9));
                    return Boolean.FALSE;
                });

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinator<>(
                synchronizedKeyToSegmentMap, partitionRuntime,
                splitCoordinator, scheduledTask::set, failure -> {
                }, () -> {
                }, stats, nowNanos::get);

        coordinator.handleSplitCandidate(segment, 100L);
        nowNanos.set(TimeUnit.MILLISECONDS.toNanos(5));

        scheduledTask.get().run();

        org.junit.jupiter.api.Assertions.assertEquals(3_000L,
                stats.getSplitTaskStartDelayP95Micros());
        org.junit.jupiter.api.Assertions.assertEquals(4_000L,
                stats.getSplitTaskRunLatencyP95Micros());
    }

    @Test
    void splitSchedulingPauseSkipsNewCandidate() {
        final SegmentId segmentId = SegmentId.of(17);
        when(segment.getId()).thenReturn(segmentId);
        when(segment.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinator<>(
                synchronizedKeyToSegmentMap, partitionRuntime,
                splitCoordinator, Runnable::run, failure -> {
                }, () -> {
                });

        final boolean scheduled = coordinator
                .runWithSplitSchedulingPaused(
                        () -> coordinator.handleSplitCandidate(segment, 100L));

        org.junit.jupiter.api.Assertions.assertFalse(scheduled);
        verifyNoInteractions(splitCoordinator);
    }

    @Test
    void successfulSplitRequestsBackgroundRescan() {
        final SegmentId segmentId = SegmentId.of(2);
        final AtomicInteger splitAppliedCallbacks = new AtomicInteger();
        final AtomicReference<Runnable> scheduledTask = new AtomicReference<>();
        when(segment.getId()).thenReturn(segmentId);
        when(segment.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(segment.getNumberOfKeysInCache()).thenReturn(101L);
        when(splitCoordinator.optionallySplit(eq(segment), eq(100L), any()))
                .thenReturn(Boolean.TRUE);

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinator<>(
                synchronizedKeyToSegmentMap, partitionRuntime,
                splitCoordinator, scheduledTask::set, failure -> {
                }, splitAppliedCallbacks::incrementAndGet);

        coordinator.handleSplitCandidate(segment, 100L);
        scheduledTask.get().run();

        org.junit.jupiter.api.Assertions.assertEquals(1,
                splitAppliedCallbacks.get());
    }

    @Test
    void cooldownSkipsRepeatedSplitCandidateWithoutGrowth() {
        final SegmentId segmentId = SegmentId.of(3);
        final AtomicReference<Runnable> scheduledTask = new AtomicReference<>();
        final AtomicInteger scheduledCount = new AtomicInteger();
        final AtomicLong nowNanos = new AtomicLong();
        final Executor splitExecutor = task -> {
            scheduledCount.incrementAndGet();
            scheduledTask.set(task);
        };
        when(segment.getId()).thenReturn(segmentId);
        when(segment.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(segment.getNumberOfKeysInCache()).thenReturn(101L);
        when(splitCoordinator.optionallySplit(eq(segment), eq(100L), any()))
                .thenReturn(Boolean.FALSE);

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinator<>(
                synchronizedKeyToSegmentMap, partitionRuntime,
                splitCoordinator, splitExecutor, failure -> {
                }, () -> {
                }, nowNanos::get);

        coordinator.handleSplitCandidate(segment, 100L);
        scheduledTask.get().run();

        final boolean rescheduled = coordinator.handleSplitCandidate(segment,
                100L);

        org.junit.jupiter.api.Assertions.assertFalse(rescheduled);
        org.junit.jupiter.api.Assertions.assertEquals(1,
                scheduledCount.get());
    }

    @Test
    void growthBypassesCooldownForHotSegment() {
        final SegmentId segmentId = SegmentId.of(4);
        final AtomicReference<Runnable> scheduledTask = new AtomicReference<>();
        final AtomicInteger scheduledCount = new AtomicInteger();
        final AtomicLong nowNanos = new AtomicLong();
        final AtomicLong currentKeys = new AtomicLong(101L);
        final Executor splitExecutor = task -> {
            scheduledCount.incrementAndGet();
            scheduledTask.set(task);
        };
        when(segment.getId()).thenReturn(segmentId);
        when(segment.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(segment.getNumberOfKeysInCache())
                .thenAnswer(invocation -> currentKeys.get());
        when(splitCoordinator.optionallySplit(eq(segment), eq(100L), any()))
                .thenReturn(Boolean.FALSE);

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinator<>(
                synchronizedKeyToSegmentMap, partitionRuntime,
                splitCoordinator, splitExecutor, failure -> {
                }, () -> {
                }, nowNanos::get);

        coordinator.handleSplitCandidate(segment, 100L);
        scheduledTask.get().run();

        currentKeys.set(112L);
        final boolean rescheduled = coordinator.handleSplitCandidate(segment,
                100L);

        org.junit.jupiter.api.Assertions.assertTrue(rescheduled);
        org.junit.jupiter.api.Assertions.assertEquals(2,
                scheduledCount.get());
    }

    @Test
    void cooldownExpiresAndAllowsRetryWithoutAdditionalGrowth() {
        final SegmentId segmentId = SegmentId.of(5);
        final AtomicReference<Runnable> scheduledTask = new AtomicReference<>();
        final AtomicInteger scheduledCount = new AtomicInteger();
        final AtomicLong nowNanos = new AtomicLong();
        final Executor splitExecutor = task -> {
            scheduledCount.incrementAndGet();
            scheduledTask.set(task);
        };
        when(segment.getId()).thenReturn(segmentId);
        when(segment.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(segment.getNumberOfKeysInCache()).thenReturn(101L);
        when(splitCoordinator.optionallySplit(eq(segment), eq(100L), any()))
                .thenReturn(Boolean.FALSE);

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinator<>(
                synchronizedKeyToSegmentMap, partitionRuntime,
                splitCoordinator, splitExecutor, failure -> {
                }, () -> {
                }, nowNanos::get);

        coordinator.handleSplitCandidate(segment, 100L);
        scheduledTask.get().run();

        nowNanos.set(600_000_000L);
        final boolean rescheduled = coordinator.handleSplitCandidate(segment,
                100L);

        org.junit.jupiter.api.Assertions.assertTrue(rescheduled);
        org.junit.jupiter.api.Assertions.assertEquals(2,
                scheduledCount.get());
    }

    @Test
    void longSplitAttemptExtendsAdaptiveCooldown() {
        final SegmentId segmentId = SegmentId.of(6);
        final AtomicReference<Runnable> scheduledTask = new AtomicReference<>();
        final AtomicInteger scheduledCount = new AtomicInteger();
        final AtomicLong nowNanos = new AtomicLong();
        final Executor splitExecutor = task -> {
            scheduledCount.incrementAndGet();
            scheduledTask.set(task);
        };
        when(segment.getId()).thenReturn(segmentId);
        when(segment.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(segment.getNumberOfKeysInCache()).thenReturn(101L);
        when(splitCoordinator.optionallySplit(eq(segment), eq(100L), any()))
                .thenAnswer(invocation -> {
                    nowNanos.addAndGet(TimeUnit.SECONDS.toNanos(2L));
                    return Boolean.FALSE;
                });

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinator<>(
                synchronizedKeyToSegmentMap, partitionRuntime,
                splitCoordinator, splitExecutor, failure -> {
                }, () -> {
                }, nowNanos::get);

        coordinator.handleSplitCandidate(segment, 100L);
        scheduledTask.get().run();

        nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(600L));
        final boolean blockedByAdaptiveCooldown = coordinator
                .handleSplitCandidate(segment, 100L);

        org.junit.jupiter.api.Assertions
                .assertFalse(blockedByAdaptiveCooldown);
        org.junit.jupiter.api.Assertions.assertEquals(1,
                scheduledCount.get());

        nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(800L));
        final boolean rescheduled = coordinator.handleSplitCandidate(segment,
                100L);

        org.junit.jupiter.api.Assertions.assertTrue(rescheduled);
        org.junit.jupiter.api.Assertions.assertEquals(2,
                scheduledCount.get());
    }

}
