package org.hestiastore.index.segmentindex.core.split;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.SegmentIndexRuntimeState;
import org.hestiastore.index.segmentindex.core.routing.MappedSegmentLeaseService;
import org.hestiastore.index.segmentindex.core.routing.RouteSplitLease;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentindex.routemap.PersistentSegmentRouteMap;
import org.hestiastore.index.segmentindex.routemap.RouteSplitPlan;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SplitTaskCoordinatorTest {

    @Mock
    private PersistentSegmentRouteMap<String> keyToSegmentMap;

    private SegmentRouteMap<String> synchronizedKeyToSegmentMap;

    @Mock
    private Segment<String, String> segment;

    @Mock
    private BlockingSegment<String, String> segmentHandle;

    @Mock
    private BlockingSegment.Runtime runtime;

    @Mock
    private RouteSplitPlanner<String, String> splitCoordinator;

    @Mock
    private RouteSplitPublisher<String, String> splitPublishCoordinator;

    @Mock
    private RouteSplitPlan<String> splitPlan;

    @Mock
    private MappedSegmentLeaseService<String, String> segmentLeaseService;

    @Mock
    private RouteSplitLease<String, String> splitLease;

    @BeforeEach
    void setUp() {
        synchronizedKeyToSegmentMap = keyToSegmentMap;
    }

    @Test
    void returnsEarlyWhenSegmentIsClosed() {
        final SegmentId segmentId = SegmentId.of(1);
        when(runtime.getState()).thenReturn(SegmentState.CLOSED);
        allowSplitLease(segmentId);

        final SplitTaskCoordinator<String, String> coordinator = newCoordinator();

        coordinator.scheduleEligibleSplit(segmentId, 100L, 101L);

        verifyNoInteractions(keyToSegmentMap, splitCoordinator);
        verify(runtime, never()).getNumberOfKeysInCache();
    }

    @Test
    void scheduleEligibleSplitSkipsUnavailableSplitLease() {
        final SegmentId segmentId = SegmentId.of(1);
        when(segmentLeaseService.tryAcquireForSplit(segmentId))
                .thenReturn(Optional.empty());

        final SplitTaskCoordinator<String, String> coordinator = newCoordinator();

        coordinator.scheduleEligibleSplit(segmentId, 1000L, 50L);

        verifyNoInteractions(splitCoordinator);
    }

    @Test
    void schedulesSplitAfterDrainForStillMappedSegment() {
        final SegmentId segmentId = SegmentId.of(1);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        allowSplitLease(segmentId);

        final SplitTaskCoordinator<String, String> coordinator = newCoordinator();

        coordinator.scheduleEligibleSplit(segmentId, 100L, 101L);

        verify(splitCoordinator).tryPrepareSplit(segmentHandle, 100L);
    }

    @Test
    void scheduleEligibleSplitUsesObservedKeyCountWithoutRecounting() {
        final SegmentId segmentId = SegmentId.of(12);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        allowSplitLease(segmentId);

        final SplitTaskCoordinator<String, String> coordinator = newCoordinator();

        coordinator.scheduleEligibleSplit(segmentId, 100L, 101L);

        verify(splitCoordinator).tryPrepareSplit(segmentHandle, 100L);
        verify(runtime, never()).getNumberOfKeysInCache();
    }

    @Test
    void schedulesSplitOnDedicatedExecutor() {
        final SegmentId segmentId = SegmentId.of(1);
        final AtomicReference<Runnable> scheduledTask = new AtomicReference<>();
        final Executor splitExecutor = scheduledTask::set;
        when(runtime.getState()).thenReturn(SegmentState.READY);
        allowSplitLease(segmentId);

        final SplitTaskCoordinator<String, String> coordinator = newCoordinator(splitExecutor);

        coordinator.scheduleEligibleSplit(segmentId, 100L, 101L);

        verifyNoInteractions(splitCoordinator);
        final Runnable task = scheduledTask.get();
        org.junit.jupiter.api.Assertions.assertNotNull(task);
        org.junit.jupiter.api.Assertions.assertEquals(1,
                coordinator.splitInFlightCount());

        task.run();

        verify(splitCoordinator).tryPrepareSplit(segmentHandle, 100L);
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
        final SplitStatsRecorder statsRecorder = new SplitStatsRecorder();
        when(runtime.getState()).thenReturn(SegmentState.READY);
        allowSplitLease(segmentId);
        when(splitCoordinator.tryPrepareSplit(segmentHandle, 100L))
                .thenAnswer(invocation -> {
                    nowNanos.set(TimeUnit.MILLISECONDS.toNanos(9));
                    return null;
                });

        final SplitTaskCoordinator<String, String> coordinator = newCoordinator(scheduledTask::set, statsRecorder,
                nowNanos::get);

        coordinator.scheduleEligibleSplit(segmentId, 100L, 101L);
        nowNanos.set(TimeUnit.MILLISECONDS.toNanos(5));

        scheduledTask.get().run();

        final SplitStats stats = statsRecorder.statsSnapshot(0, 0);
        org.junit.jupiter.api.Assertions.assertEquals(3_000L,
                stats.splitTaskStartDelayP95Micros());
        org.junit.jupiter.api.Assertions.assertEquals(4_000L,
                stats.splitTaskRunLatencyP95Micros());
    }

    @Test
    void successfulSplitPublishesPreparedPlan() {
        final SegmentId segmentId = SegmentId.of(2);
        final AtomicReference<Runnable> scheduledTask = new AtomicReference<>();
        when(runtime.getState()).thenReturn(SegmentState.READY);
        allowSplitLease(segmentId);
        when(splitCoordinator.tryPrepareSplit(segmentHandle, 100L))
                .thenReturn(splitPlan);
        when(splitPublishCoordinator.applyPreparedSplit(splitPlan))
                .thenReturn(Boolean.TRUE);

        final SplitTaskCoordinator<String, String> coordinator = newCoordinator(scheduledTask::set);

        coordinator.scheduleEligibleSplit(segmentId, 100L, 101L);
        scheduledTask.get().run();

        verify(splitPublishCoordinator).applyPreparedSplit(splitPlan);
        verify(splitLease).completeAfterPublish();
        verify(splitLease, never()).abort();
    }

    @Test
    void failedPublishAbortsPreparedSplit() {
        final SegmentId segmentId = SegmentId.of(16);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(splitLease.segmentId()).thenReturn(segmentId);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        allowSplitLease(segmentId);
        when(splitCoordinator.tryPrepareSplit(segmentHandle, 100L))
                .thenReturn(splitPlan);
        when(splitPublishCoordinator.applyPreparedSplit(splitPlan))
                .thenReturn(Boolean.FALSE);

        final SplitTaskCoordinator<String, String> coordinator = newCoordinator();

        coordinator.scheduleEligibleSplit(segmentId, 100L, 101L);

        verify(splitPublishCoordinator).applyPreparedSplit(splitPlan);
        verify(splitLease).abort();
        verify(splitLease, never()).completeAfterPublish();
    }

    @Test
    void persistenceFailureAfterMapPublishCompletesDrainAndFailsCoordinator() {
        final SegmentId segmentId = SegmentId.of(19);
        final AtomicReference<Runnable> scheduledTask = new AtomicReference<>();
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(splitLease.segmentId()).thenReturn(segmentId);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of());
        allowSplitLease(segmentId);
        when(splitCoordinator.tryPrepareSplit(segmentHandle, 100L))
                .thenReturn(splitPlan);
        when(splitPublishCoordinator.applyPreparedSplit(splitPlan))
                .thenThrow(new IllegalStateException("flush failed"));

        final SplitTaskCoordinator<String, String> coordinator = newCoordinator(scheduledTask::set);

        coordinator.scheduleEligibleSplit(segmentId, 100L, 101L);
        scheduledTask.get().run();

        assertThrows(IllegalStateException.class,
                () -> coordinator.awaitSplitsIdle(1_000L));
        verify(splitLease).completeAfterPublish();
        verify(splitLease, never()).abort();
    }

    @Test
    void splitLeaseCompletionFailureAfterPublishFailsCoordinator() {
        final SegmentId segmentId = SegmentId.of(20);
        final AtomicReference<Runnable> scheduledTask = new AtomicReference<>();
        when(runtime.getState()).thenReturn(SegmentState.READY);
        allowSplitLease(segmentId);
        when(splitCoordinator.tryPrepareSplit(segmentHandle, 100L))
                .thenReturn(splitPlan);
        when(splitPublishCoordinator.applyPreparedSplit(splitPlan))
                .thenReturn(Boolean.TRUE);
        doThrow(new IllegalStateException("topology failed"))
                .when(splitLease).completeAfterPublish();

        final SplitTaskCoordinator<String, String> coordinator = newCoordinator(scheduledTask::set);

        coordinator.scheduleEligibleSplit(segmentId, 100L, 101L);
        scheduledTask.get().run();

        assertThrows(IllegalStateException.class,
                () -> coordinator.awaitSplitsIdle(1_000L));
        verify(splitLease).completeAfterPublish();
        verify(splitLease, never()).abort();
    }

    @Test
    void fatalPublishFailureTransitionsCoordinatorToFailure() {
        final SegmentId segmentId = SegmentId.of(18);
        final AtomicReference<Runnable> scheduledTask = new AtomicReference<>();
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(splitLease.segmentId()).thenReturn(segmentId);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        allowSplitLease(segmentId);
        when(splitCoordinator.tryPrepareSplit(segmentHandle, 100L))
                .thenReturn(splitPlan);
        when(splitPublishCoordinator.applyPreparedSplit(splitPlan))
                .thenThrow(new IllegalStateException("publish failed"));

        final SplitTaskCoordinator<String, String> coordinator = newCoordinator(scheduledTask::set);

        coordinator.scheduleEligibleSplit(segmentId, 100L, 101L);
        scheduledTask.get().run();

        assertThrows(IllegalStateException.class,
                () -> coordinator.awaitSplitsIdle(1_000L));
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
        when(runtime.getState()).thenReturn(SegmentState.READY);
        allowSplitLease(segmentId);
        when(splitCoordinator.tryPrepareSplit(segmentHandle, 100L))
                .thenReturn(null);

        final SplitTaskCoordinator<String, String> coordinator = newCoordinator(splitExecutor, nowNanos::get);

        coordinator.scheduleEligibleSplit(segmentId, 100L, 101L);
        scheduledTask.get().run();

        final boolean rescheduled = coordinator
                .scheduleEligibleSplit(segmentId, 100L, 101L);

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
        when(runtime.getState()).thenReturn(SegmentState.READY);
        allowSplitLease(segmentId);
        when(splitCoordinator.tryPrepareSplit(segmentHandle, 100L))
                .thenReturn(null);

        final SplitTaskCoordinator<String, String> coordinator = newCoordinator(splitExecutor, nowNanos::get);

        coordinator.scheduleEligibleSplit(segmentId, 100L,
                currentKeys.get());
        scheduledTask.get().run();

        currentKeys.set(112L);
        final boolean rescheduled = coordinator
                .scheduleEligibleSplit(segmentId, 100L,
                        currentKeys.get());

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
        when(runtime.getState()).thenReturn(SegmentState.READY);
        allowSplitLease(segmentId);
        when(splitCoordinator.tryPrepareSplit(segmentHandle, 100L))
                .thenReturn(null);

        final SplitTaskCoordinator<String, String> coordinator = newCoordinator(splitExecutor, nowNanos::get);

        coordinator.scheduleEligibleSplit(segmentId, 100L, 101L);
        scheduledTask.get().run();

        nowNanos.set(600_000_000L);
        final boolean rescheduled = coordinator
                .scheduleEligibleSplit(segmentId, 100L, 101L);

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
        when(runtime.getState()).thenReturn(SegmentState.READY);
        allowSplitLease(segmentId);
        when(splitCoordinator.tryPrepareSplit(segmentHandle, 100L))
                .thenAnswer(invocation -> {
                    nowNanos.addAndGet(TimeUnit.SECONDS.toNanos(2L));
                    return null;
                });

        final SplitTaskCoordinator<String, String> coordinator = newCoordinator(splitExecutor, nowNanos::get);

        coordinator.scheduleEligibleSplit(segmentId, 100L, 101L);
        scheduledTask.get().run();

        nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(600L));
        final boolean blockedByAdaptiveCooldown = coordinator
                .scheduleEligibleSplit(segmentId, 100L, 101L);

        org.junit.jupiter.api.Assertions
                .assertFalse(blockedByAdaptiveCooldown);
        org.junit.jupiter.api.Assertions.assertEquals(1,
                scheduledCount.get());

        nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(800L));
        final boolean rescheduled = coordinator
                .scheduleEligibleSplit(segmentId, 100L, 101L);

        org.junit.jupiter.api.Assertions.assertTrue(rescheduled);
        org.junit.jupiter.api.Assertions.assertEquals(2,
                scheduledCount.get());
    }

    private SplitTaskCoordinator<String, String> newCoordinator() {
        return newCoordinator(Runnable::run);
    }

    private SplitTaskCoordinator<String, String> newCoordinator(
            final Executor splitExecutor) {
        return new SplitTaskCoordinator<>(synchronizedKeyToSegmentMap,
                segmentLeaseService, splitCoordinator,
                splitPublishCoordinator, splitExecutor,
                runtimeState(), new SplitStatsRecorder(),
                System::nanoTime);
    }

    private SplitTaskCoordinator<String, String> newCoordinator(
            final Executor splitExecutor,
            final SplitStatsRecorder statsRecorder,
            final java.util.function.LongSupplier nanoTimeSupplier) {
        return new SplitTaskCoordinator<>(synchronizedKeyToSegmentMap,
                segmentLeaseService, splitCoordinator,
                splitPublishCoordinator, splitExecutor,
                runtimeState(), statsRecorder, nanoTimeSupplier);
    }

    private SplitTaskCoordinator<String, String> newCoordinator(
            final Executor splitExecutor,
            final java.util.function.LongSupplier nanoTimeSupplier) {
        return new SplitTaskCoordinator<>(synchronizedKeyToSegmentMap,
                segmentLeaseService, splitCoordinator,
                splitPublishCoordinator, splitExecutor,
                runtimeState(), new SplitStatsRecorder(),
                nanoTimeSupplier);
    }

    private SegmentIndexRuntimeState runtimeState() {
        return new SegmentIndexRuntimeState() {

            @Override
            public SegmentIndexState currentState() {
                return SegmentIndexState.READY;
            }

            @Override
            public void markRuntimeFailure(final RuntimeException failure) {
            }
        };
    }

    private void allowSplitLease(final SegmentId segmentId) {
        when(segmentLeaseService.tryAcquireForSplit(segmentId))
                .thenReturn(Optional.of(splitLease));
        when(splitLease.segment()).thenReturn(segmentHandle);
        when(segmentHandle.getRuntime()).thenReturn(runtime);
    }
}
