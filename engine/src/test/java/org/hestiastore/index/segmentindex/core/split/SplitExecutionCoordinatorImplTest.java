package org.hestiastore.index.segmentindex.core.split;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;

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
class SplitExecutionCoordinatorImplTest {

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
    private RouteSplitPublishCoordinator<String, String> splitPublishCoordinator;

    @Mock
    private RouteSplitPlan<String> splitPlan;

    @BeforeEach
    void setUp() {
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
        when(segmentHandle.getRuntime()).thenReturn(runtime);
    }

    @Test
    void returnsEarlyWhenSegmentIsClosed() {
        when(runtime.getState()).thenReturn(SegmentState.CLOSED);

        final SplitExecutionCoordinator<String, String> coordinator =
                newCoordinator();

        coordinator.scheduleEligibleSplit(segmentHandle, 100L, 101L);

        verifyNoInteractions(keyToSegmentMap, splitCoordinator);
        verify(runtime, never()).getNumberOfKeysInCache();
    }

    @Test
    void scheduleEligibleSplitSkipsUnmappedSegment() {
        final SegmentId segmentId = SegmentId.of(1);
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of());

        final SplitExecutionCoordinator<String, String> coordinator =
                newCoordinator();

        coordinator.scheduleEligibleSplit(segmentHandle, 1000L, 50L);

        verifyNoInteractions(splitCoordinator);
    }

    @Test
    void schedulesSplitAfterDrainForStillMappedSegment() {
        final SegmentId segmentId = SegmentId.of(1);
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));

        final SplitExecutionCoordinator<String, String> coordinator =
                newCoordinator();

        coordinator.scheduleEligibleSplit(segmentHandle, 100L, 101L);

        verify(splitCoordinator).tryPrepareSplit(eq(segmentHandle), eq(100L));
    }

    @Test
    void scheduleEligibleSplitUsesObservedKeyCountWithoutRecounting() {
        final SegmentId segmentId = SegmentId.of(12);
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));

        final SplitExecutionCoordinator<String, String> coordinator =
                newCoordinator();

        coordinator.scheduleEligibleSplit(segmentHandle, 100L, 101L);

        verify(splitCoordinator).tryPrepareSplit(eq(segmentHandle), eq(100L));
        verify(runtime, never()).getNumberOfKeysInCache();
    }

    @Test
    void schedulesSplitOnDedicatedExecutor() {
        final SegmentId segmentId = SegmentId.of(1);
        final AtomicReference<Runnable> scheduledTask = new AtomicReference<>();
        final Executor splitExecutor = scheduledTask::set;
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));

        final SplitExecutionCoordinator<String, String> coordinator =
                newCoordinator(splitExecutor);

        coordinator.scheduleEligibleSplit(segmentHandle, 100L, 101L);

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
        final AtomicLong splitTaskStartDelayMicros = new AtomicLong();
        final AtomicLong splitTaskRunLatencyMicros = new AtomicLong();
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(splitCoordinator.tryPrepareSplit(eq(segmentHandle), eq(100L)))
                .thenAnswer(invocation -> {
                    nowNanos.set(TimeUnit.MILLISECONDS.toNanos(9));
                    return null;
                });

        final SplitExecutionCoordinator<String, String> coordinator =
                newCoordinator(scheduledTask::set,
                        nanos -> splitTaskStartDelayMicros.set(
                                TimeUnit.NANOSECONDS.toMicros(nanos)),
                        nanos -> splitTaskRunLatencyMicros.set(
                                TimeUnit.NANOSECONDS.toMicros(nanos)),
                        nowNanos::get);

        coordinator.scheduleEligibleSplit(segmentHandle, 100L, 101L);
        nowNanos.set(TimeUnit.MILLISECONDS.toNanos(5));

        scheduledTask.get().run();

        org.junit.jupiter.api.Assertions.assertEquals(3_000L,
                splitTaskStartDelayMicros.get());
        org.junit.jupiter.api.Assertions.assertEquals(4_000L,
                splitTaskRunLatencyMicros.get());
    }

    @Test
    void splitSchedulingPauseSkipsNewCandidate() {
        final SegmentId segmentId = SegmentId.of(17);
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));

        final SplitExecutionCoordinator<String, String> coordinator =
                newCoordinator();

        final boolean scheduled = coordinator
                .runWithSplitSchedulingPaused(
                        () -> coordinator.scheduleEligibleSplit(segmentHandle,
                                100L, 101L));

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
        when(splitCoordinator.tryPrepareSplit(eq(segmentHandle), eq(100L)))
                .thenReturn(splitPlan);
        when(splitPublishCoordinator.applyPreparedSplit(splitPlan))
                .thenReturn(Boolean.TRUE);

        final SplitExecutionCoordinator<String, String> coordinator =
                newCoordinator(scheduledTask::set,
                        (Runnable) () -> splitAppliedCallbacks
                                .incrementAndGet());

        coordinator.scheduleEligibleSplit(segmentHandle, 100L, 101L);
        scheduledTask.get().run();

        org.junit.jupiter.api.Assertions.assertEquals(1,
                splitAppliedCallbacks.get());
        verify(splitPublishCoordinator).applyPreparedSplit(splitPlan);
    }

    @Test
    void failedPublishAbortsPreparedSplit() {
        final SegmentId segmentId = SegmentId.of(16);
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(splitCoordinator.tryPrepareSplit(eq(segmentHandle), eq(100L)))
                .thenReturn(splitPlan);
        when(splitPublishCoordinator.applyPreparedSplit(splitPlan))
                .thenReturn(Boolean.FALSE);

        final SplitExecutionCoordinator<String, String> coordinator =
                newCoordinator();

        coordinator.scheduleEligibleSplit(segmentHandle, 100L, 101L);

        verify(splitPublishCoordinator).applyPreparedSplit(splitPlan);
    }

    @Test
    void fatalPublishFailureTransitionsCoordinatorToFailure() {
        final SegmentId segmentId = SegmentId.of(18);
        final AtomicReference<Runnable> scheduledTask = new AtomicReference<>();
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(splitCoordinator.tryPrepareSplit(eq(segmentHandle), eq(100L)))
                .thenReturn(splitPlan);
        when(splitPublishCoordinator.applyPreparedSplit(splitPlan))
                .thenThrow(new IllegalStateException("publish failed"));

        final SplitExecutionCoordinator<String, String> coordinator =
                newCoordinator(scheduledTask::set);

        coordinator.scheduleEligibleSplit(segmentHandle, 100L, 101L);
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
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(splitCoordinator.tryPrepareSplit(eq(segmentHandle), eq(100L)))
                .thenReturn(null);

        final SplitExecutionCoordinator<String, String> coordinator =
                newCoordinator(splitExecutor, nowNanos::get);

        coordinator.scheduleEligibleSplit(segmentHandle, 100L, 101L);
        scheduledTask.get().run();

        final boolean rescheduled = coordinator
                .scheduleEligibleSplit(segmentHandle, 100L, 101L);

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
        when(splitCoordinator.tryPrepareSplit(eq(segmentHandle), eq(100L)))
                .thenReturn(null);

        final SplitExecutionCoordinator<String, String> coordinator =
                newCoordinator(splitExecutor, nowNanos::get);

        coordinator.scheduleEligibleSplit(segmentHandle, 100L,
                currentKeys.get());
        scheduledTask.get().run();

        currentKeys.set(112L);
        final boolean rescheduled = coordinator
                .scheduleEligibleSplit(segmentHandle, 100L,
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
        when(segmentHandle.getId()).thenReturn(segmentId);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(splitCoordinator.tryPrepareSplit(eq(segmentHandle), eq(100L)))
                .thenReturn(null);

        final SplitExecutionCoordinator<String, String> coordinator =
                newCoordinator(splitExecutor, nowNanos::get);

        coordinator.scheduleEligibleSplit(segmentHandle, 100L, 101L);
        scheduledTask.get().run();

        nowNanos.set(600_000_000L);
        final boolean rescheduled = coordinator
                .scheduleEligibleSplit(segmentHandle, 100L, 101L);

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
        when(splitCoordinator.tryPrepareSplit(eq(segmentHandle), eq(100L)))
                .thenAnswer(invocation -> {
                    nowNanos.addAndGet(TimeUnit.SECONDS.toNanos(2L));
                    return null;
                });

        final SplitExecutionCoordinator<String, String> coordinator =
                newCoordinator(splitExecutor, nowNanos::get);

        coordinator.scheduleEligibleSplit(segmentHandle, 100L, 101L);
        scheduledTask.get().run();

        nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(600L));
        final boolean blockedByAdaptiveCooldown = coordinator
                .scheduleEligibleSplit(segmentHandle, 100L, 101L);

        org.junit.jupiter.api.Assertions
                .assertFalse(blockedByAdaptiveCooldown);
        org.junit.jupiter.api.Assertions.assertEquals(1,
                scheduledCount.get());

        nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(800L));
        final boolean rescheduled = coordinator
                .scheduleEligibleSplit(segmentHandle, 100L, 101L);

        org.junit.jupiter.api.Assertions.assertTrue(rescheduled);
        org.junit.jupiter.api.Assertions.assertEquals(2,
                scheduledCount.get());
    }

    private SplitExecutionCoordinator<String, String> newCoordinator() {
        return newCoordinator(Runnable::run);
    }

    private SplitExecutionCoordinator<String, String> newCoordinator(
            final Executor splitExecutor) {
        return new SplitExecutionCoordinatorImpl<>(synchronizedKeyToSegmentMap,
                splitCoordinator, splitPublishCoordinator, splitExecutor,
                SplitFailureReporter.noOp(), () -> {
                });
    }

    private SplitExecutionCoordinator<String, String> newCoordinator(
            final Executor splitExecutor,
            final Runnable onSplitApplied) {
        return new SplitExecutionCoordinatorImpl<>(synchronizedKeyToSegmentMap,
                splitCoordinator, splitPublishCoordinator, splitExecutor,
                SplitFailureReporter.noOp(), onSplitApplied);
    }

    private SplitExecutionCoordinator<String, String> newCoordinator(
            final Executor splitExecutor,
            final LongConsumer splitTaskStartDelayRecorder,
            final LongConsumer splitTaskRunLatencyRecorder,
            final java.util.function.LongSupplier nanoTimeSupplier) {
        return new SplitExecutionCoordinatorImpl<>(synchronizedKeyToSegmentMap,
                splitCoordinator, splitPublishCoordinator, splitExecutor,
                SplitFailureReporter.noOp(), () -> {
                }, new SplitTelemetry() {
                    @Override
                    public void recordSplitScheduled() {
                    }

                    @Override
                    public void recordSplitTaskStartDelayNanos(
                            final long nanos) {
                        splitTaskStartDelayRecorder.accept(nanos);
                    }

                    @Override
                    public void recordSplitTaskRunLatencyNanos(
                            final long nanos) {
                        splitTaskRunLatencyRecorder.accept(nanos);
                    }
                }, nanoTimeSupplier);
    }

    private SplitExecutionCoordinator<String, String> newCoordinator(
            final Executor splitExecutor,
            final java.util.function.LongSupplier nanoTimeSupplier) {
        return new SplitExecutionCoordinatorImpl<>(synchronizedKeyToSegmentMap,
                splitCoordinator, splitPublishCoordinator, splitExecutor,
                SplitFailureReporter.noOp(), () -> {
                },
                nanoTimeSupplier);
    }
}
