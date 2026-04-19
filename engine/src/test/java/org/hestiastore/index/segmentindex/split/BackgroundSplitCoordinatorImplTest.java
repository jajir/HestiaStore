package org.hestiastore.index.segmentindex.split;

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
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapImpl;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentregistry.SegmentHandle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BackgroundSplitCoordinatorImplTest {

    @Mock
    private KeyToSegmentMapImpl<String> keyToSegmentMap;

    private KeyToSegmentMap<String> synchronizedKeyToSegmentMap;

    @Mock
    private Segment<String, String> segment;

    @Mock
    private SegmentHandle<String, String> segmentHandle;

    @Mock
    private SegmentHandle.Runtime runtime;

    @Mock
    private RouteSplitCoordinator<String, String> splitCoordinator;

    @Mock
    private PreparedRouteSplit<String> preparedSplit;

    @BeforeEach
    void setUp() {
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
        when(segmentHandle.getRuntime()).thenReturn(runtime);
    }

    @Test
    void returnsEarlyWhenSegmentIsClosed() {
        when(runtime.getState()).thenReturn(SegmentState.CLOSED);

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinatorImpl<>(
                synchronizedKeyToSegmentMap, splitCoordinator, Runnable::run,
                failure -> {
                }, () -> {
                });

        coordinator.handleSplitCandidate(segmentHandle, 100L, false);

        verifyNoInteractions(keyToSegmentMap, splitCoordinator);
        verify(runtime, never()).getNumberOfKeysInCache();
    }

    @Test
    void usesSegmentSizeThreshold_notSegmentCacheThreshold() {
        final SegmentId segmentId = SegmentId.of(1);
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(runtime.getNumberOfKeysInCache()).thenReturn(50L);

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinatorImpl<>(
                synchronizedKeyToSegmentMap, splitCoordinator, Runnable::run,
                failure -> {
                }, () -> {
                });

        coordinator.handleSplitCandidate(segmentHandle, 1000L, false);

        verify(runtime).getNumberOfKeysInCache();
        verifyNoInteractions(splitCoordinator);
    }

    @Test
    void schedulesSplitAfterDrainForStillMappedSegment() {
        final SegmentId segmentId = SegmentId.of(1);
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(runtime.getNumberOfKeysInCache()).thenReturn(101L);

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinatorImpl<>(
                synchronizedKeyToSegmentMap, splitCoordinator, Runnable::run,
                failure -> {
                }, () -> {
                });

        coordinator.handleSplitCandidate(segmentHandle, 100L, false);

        verify(splitCoordinator).tryPrepareSplit(eq(segmentHandle), eq(100L));
    }

    @Test
    void schedulesSplitOnDedicatedExecutor() {
        final SegmentId segmentId = SegmentId.of(1);
        final AtomicReference<Runnable> scheduledTask = new AtomicReference<>();
        final Executor splitExecutor = scheduledTask::set;
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(runtime.getNumberOfKeysInCache()).thenReturn(101L);

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinatorImpl<>(
                synchronizedKeyToSegmentMap, splitCoordinator, splitExecutor,
                failure -> {
                }, () -> {
                });

        coordinator.handleSplitCandidate(segmentHandle, 100L, false);

        verifyNoInteractions(splitCoordinator);
        final Runnable task = scheduledTask.get();
        org.junit.jupiter.api.Assertions.assertNotNull(task);
        org.junit.jupiter.api.Assertions.assertEquals(1,
                coordinator.splitInFlightCount());

        task.run();

        verify(splitCoordinator).tryPrepareSplit(eq(segmentHandle), eq(100L));
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
        final TestBackgroundSplitMetrics metrics = new TestBackgroundSplitMetrics();
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(runtime.getNumberOfKeysInCache()).thenReturn(101L);
        when(splitCoordinator.tryPrepareSplit(eq(segmentHandle), eq(100L)))
                .thenAnswer(invocation -> {
                    nowNanos.set(TimeUnit.MILLISECONDS.toNanos(9));
                    return null;
                });

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinatorImpl<>(
                synchronizedKeyToSegmentMap, splitCoordinator, scheduledTask::set,
                failure -> {
                }, () -> {
                }, metrics, nowNanos::get);

        coordinator.handleSplitCandidate(segmentHandle, 100L, false);
        nowNanos.set(TimeUnit.MILLISECONDS.toNanos(5));

        scheduledTask.get().run();

        org.junit.jupiter.api.Assertions.assertEquals(3_000L,
                metrics.getSplitTaskStartDelayMicros());
        org.junit.jupiter.api.Assertions.assertEquals(4_000L,
                metrics.getSplitTaskRunLatencyMicros());
    }

    @Test
    void splitSchedulingPauseSkipsNewCandidate() {
        final SegmentId segmentId = SegmentId.of(17);
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinatorImpl<>(
                synchronizedKeyToSegmentMap, splitCoordinator, Runnable::run,
                failure -> {
                }, () -> {
                });

        final boolean scheduled = coordinator
                .runWithSplitSchedulingPaused(
                        () -> coordinator.handleSplitCandidate(segmentHandle,
                                100L,
                                false));

        org.junit.jupiter.api.Assertions.assertFalse(scheduled);
        verifyNoInteractions(splitCoordinator);
    }

    @Test
    void successfulSplitRequestsBackgroundRescan() {
        final SegmentId segmentId = SegmentId.of(2);
        final AtomicInteger splitAppliedCallbacks = new AtomicInteger();
        final AtomicReference<Runnable> scheduledTask = new AtomicReference<>();
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(runtime.getNumberOfKeysInCache()).thenReturn(101L);
        when(splitCoordinator.tryPrepareSplit(eq(segmentHandle), eq(100L)))
                .thenReturn(preparedSplit);
        when(splitCoordinator.publishPreparedSplit(preparedSplit))
                .thenReturn(Boolean.TRUE);

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinatorImpl<>(
                synchronizedKeyToSegmentMap, splitCoordinator, scheduledTask::set,
                failure -> {
                }, splitAppliedCallbacks::incrementAndGet);

        coordinator.handleSplitCandidate(segmentHandle, 100L, false);
        scheduledTask.get().run();

        org.junit.jupiter.api.Assertions.assertEquals(1,
                splitAppliedCallbacks.get());
        verify(splitCoordinator).completePreparedSplit(preparedSplit);
    }

    @Test
    void failedPublishAbortsPreparedSplit() {
        final SegmentId segmentId = SegmentId.of(16);
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(runtime.getNumberOfKeysInCache()).thenReturn(101L);
        when(splitCoordinator.tryPrepareSplit(eq(segmentHandle), eq(100L)))
                .thenReturn(preparedSplit);
        when(splitCoordinator.publishPreparedSplit(preparedSplit))
                .thenReturn(Boolean.FALSE);

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinatorImpl<>(
                synchronizedKeyToSegmentMap, splitCoordinator, Runnable::run,
                failure -> {
                }, () -> {
                });

        coordinator.handleSplitCandidate(segmentHandle, 100L, false);

        verify(splitCoordinator).abortPreparedSplit(preparedSplit);
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
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(runtime.getNumberOfKeysInCache()).thenReturn(101L);
        when(splitCoordinator.tryPrepareSplit(eq(segmentHandle), eq(100L)))
                .thenReturn(null);

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinatorImpl<>(
                synchronizedKeyToSegmentMap, splitCoordinator, splitExecutor,
                failure -> {
                }, () -> {
                }, nowNanos::get);

        coordinator.handleSplitCandidate(segmentHandle, 100L, false);
        scheduledTask.get().run();

        final boolean rescheduled = coordinator
                .handleSplitCandidate(segmentHandle, 100L, false);

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
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(runtime.getNumberOfKeysInCache())
                .thenAnswer(invocation -> currentKeys.get());
        when(splitCoordinator.tryPrepareSplit(eq(segmentHandle), eq(100L)))
                .thenReturn(null);

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinatorImpl<>(
                synchronizedKeyToSegmentMap, splitCoordinator, splitExecutor,
                failure -> {
                }, () -> {
                }, nowNanos::get);

        coordinator.handleSplitCandidate(segmentHandle, 100L, false);
        scheduledTask.get().run();

        currentKeys.set(112L);
        final boolean rescheduled = coordinator
                .handleSplitCandidate(segmentHandle, 100L, false);

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
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(runtime.getNumberOfKeysInCache()).thenReturn(101L);
        when(splitCoordinator.tryPrepareSplit(eq(segmentHandle), eq(100L)))
                .thenReturn(null);

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinatorImpl<>(
                synchronizedKeyToSegmentMap, splitCoordinator, splitExecutor,
                failure -> {
                }, () -> {
                }, nowNanos::get);

        coordinator.handleSplitCandidate(segmentHandle, 100L, false);
        scheduledTask.get().run();

        nowNanos.set(600_000_000L);
        final boolean rescheduled = coordinator
                .handleSplitCandidate(segmentHandle, 100L, false);

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
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(runtime.getNumberOfKeysInCache()).thenReturn(101L);
        when(splitCoordinator.tryPrepareSplit(eq(segmentHandle), eq(100L)))
                .thenAnswer(invocation -> {
                    nowNanos.addAndGet(TimeUnit.SECONDS.toNanos(2L));
                    return null;
                });

        final BackgroundSplitCoordinator<String, String> coordinator = new BackgroundSplitCoordinatorImpl<>(
                synchronizedKeyToSegmentMap, splitCoordinator, splitExecutor,
                failure -> {
                }, () -> {
                }, nowNanos::get);

        coordinator.handleSplitCandidate(segmentHandle, 100L, false);
        scheduledTask.get().run();

        nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(600L));
        final boolean blockedByAdaptiveCooldown = coordinator
                .handleSplitCandidate(segmentHandle, 100L, false);

        org.junit.jupiter.api.Assertions
                .assertFalse(blockedByAdaptiveCooldown);
        org.junit.jupiter.api.Assertions.assertEquals(1,
                scheduledCount.get());

        nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(800L));
        final boolean rescheduled = coordinator
                .handleSplitCandidate(segmentHandle, 100L, false);

        org.junit.jupiter.api.Assertions.assertTrue(rescheduled);
        org.junit.jupiter.api.Assertions.assertEquals(2,
                scheduledCount.get());
    }

    private static final class TestBackgroundSplitMetrics
            implements BackgroundSplitMetrics {

        private long splitTaskStartDelayMicros;
        private long splitTaskRunLatencyMicros;

        @Override
        public void recordSplitTaskStartDelayNanos(final long nanos) {
            splitTaskStartDelayMicros = TimeUnit.NANOSECONDS.toMicros(nanos);
        }

        @Override
        public void recordSplitTaskRunLatencyNanos(final long nanos) {
            splitTaskRunLatencyMicros = TimeUnit.NANOSECONDS.toMicros(nanos);
        }

        private long getSplitTaskStartDelayMicros() {
            return splitTaskStartDelayMicros;
        }

        private long getSplitTaskRunLatencyMicros() {
            return splitTaskRunLatencyMicros;
        }
    }
}
