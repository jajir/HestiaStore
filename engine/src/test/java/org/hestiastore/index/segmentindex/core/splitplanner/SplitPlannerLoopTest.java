package org.hestiastore.index.segmentindex.core.splitplanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.routing.BackgroundSplitCoordinator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SplitPlannerLoopTest {

    @Mock
    private IndexConfiguration<Integer, String> conf;

    @Mock
    private RuntimeTuningState runtimeTuningState;

    @Mock
    private BackgroundSplitCoordinator<Integer, String> backgroundSplitCoordinator;

    @Mock
    private SplitTaskDispatcher<Integer, String> splitTaskDispatcher;

    private final AtomicReference<SegmentIndexState> state =
            new AtomicReference<>(SegmentIndexState.READY);

    private ExecutorService plannerExecutor;

    @BeforeEach
    void setUp() {
        plannerExecutor = Executors.newSingleThreadExecutor();
        lenient().when(conf.isBackgroundMaintenanceAutoEnabled())
                .thenReturn(Boolean.TRUE);
        lenient().when(conf.getIndexBusyTimeoutMillis()).thenReturn(100);
        lenient().when(runtimeTuningState.effectiveValue(org.hestiastore.index.control.model.RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT))
                .thenReturn(10);
        lenient().when(backgroundSplitCoordinator.splitInFlightCount())
                .thenReturn(0);
    }

    @AfterEach
    void tearDown() {
        if (plannerExecutor != null) {
            plannerExecutor.shutdownNow();
        }
    }

    @Test
    void requestRescanDispatchesFullReconciliation() throws Exception {
        final CountDownLatch dispatched = new CountDownLatch(1);
        when(splitTaskDispatcher.dispatchAllMappedCandidates(10, false))
                .thenAnswer(invocation -> {
                    dispatched.countDown();
                    return false;
                });
        final SplitPlannerLoop<Integer, String> planner = newPlanner();

        planner.requestRescan();

        assertTrue(dispatched.await(1, TimeUnit.SECONDS));
    }

    @Test
    void scheduleIfIdleSkipsWhenSplitCoordinatorIsBusy() throws Exception {
        when(backgroundSplitCoordinator.splitInFlightCount()).thenReturn(1);
        final SplitPlannerLoop<Integer, String> planner = newPlanner();

        planner.scheduleIfIdle();

        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(350L));
        verify(splitTaskDispatcher, never()).dispatchAllMappedCandidates(10,
                false);
    }

    @Test
    void hintSegmentDispatchesMappedHintsOnly() throws Exception {
        final SegmentId mappedSegmentId = SegmentId.of(7);
        final SegmentId unmappedSegmentId = SegmentId.of(8);
        final CountDownLatch dispatched = new CountDownLatch(1);
        when(splitTaskDispatcher.isCandidateMapped(mappedSegmentId))
                .thenReturn(true);
        when(splitTaskDispatcher.isCandidateMapped(unmappedSegmentId))
                .thenReturn(false);
        when(splitTaskDispatcher.dispatchCandidates(anyList(), anyInt(),
                anyBoolean())).thenAnswer(invocation -> {
                    dispatched.countDown();
                    return false;
                });
        final SplitPlannerLoop<Integer, String> planner = newPlanner();

        planner.hintSegment(mappedSegmentId);
        planner.hintSegment(unmappedSegmentId);

        assertTrue(dispatched.await(1, TimeUnit.SECONDS));
        verify(splitTaskDispatcher).dispatchCandidates(List.of(mappedSegmentId),
                10, false);
    }

    @Test
    void awaitExhaustedTriggersForceRetryPass() {
        final AtomicReference<Boolean> forceRetry = new AtomicReference<>(false);
        when(splitTaskDispatcher.dispatchAllMappedCandidates(anyInt(),
                anyBoolean())).thenAnswer(invocation -> {
                    forceRetry.set(invocation.getArgument(1));
                    return false;
                });
        final SplitPlannerLoop<Integer, String> planner = newPlanner();

        planner.requestRescan();
        awaitCondition(() -> !forceRetry.get(), 1_000L);
        planner.awaitExhausted();
        awaitCondition(() -> forceRetry.get(), 1_000L);

        assertTrue(forceRetry.get());
    }

    @Test
    void requestRescanDoesNothingWhenPlannerDisabled() throws Exception {
        when(conf.isBackgroundMaintenanceAutoEnabled()).thenReturn(Boolean.FALSE);
        final SplitPlannerLoop<Integer, String> planner = newPlanner();

        planner.requestRescan();

        Thread.yield();
        verify(splitTaskDispatcher, never()).dispatchAllMappedCandidates(10,
                false);
    }

    private SplitPlannerLoop<Integer, String> newPlanner() {
        return new SplitPlannerLoop<>(conf, runtimeTuningState,
                backgroundSplitCoordinator, splitTaskDispatcher, plannerExecutor,
                state::get, () -> {
                }, failure -> {
                }, new SplitPlannerState());
    }

    private void awaitCondition(
            final java.util.function.BooleanSupplier condition,
            final long timeoutMillis) {
        final long deadlineNanos = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadlineNanos) {
                throw new AssertionError("Condition was not met in time.");
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
        }
    }
}
