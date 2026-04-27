package org.hestiastore.index.segmentindex.core.split;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapImpl;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SplitServiceImplTest {

    @Mock
    private IndexConfiguration<String, String> conf;

    @Mock
    private RuntimeTuningState runtimeTuningState;

    @Mock
    private KeyToSegmentMapImpl<String> keyToSegmentMap;

    private KeyToSegmentMap<String> synchronizedKeyToSegmentMap;

    @Mock
    private SegmentRegistry<String, String> segmentRegistry;

    @Mock
    private SplitExecutionCoordinator<String, String> splitExecutionCoordinator;

    @Mock
    private BlockingSegment<String, String> segmentHandle;

    @Mock
    private ScheduledExecutorService splitPolicyScheduler;

    @BeforeEach
    void setUp() {
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
    }

    @Test
    void awaitQuiescenceClearsPendingRequestsWhenPolicyDisabled() {
        when(conf.isBackgroundMaintenanceAutoEnabled()).thenReturn(Boolean.TRUE);
        when(conf.getIndexBusyTimeoutMillis()).thenReturn(50);
        when(splitExecutionCoordinator.splitInFlightCount()).thenReturn(0);
        final AtomicReference<SegmentIndexState> state = new AtomicReference<>(
                SegmentIndexState.CLOSING);
        final SplitPolicyState policyState =
                new SplitPolicyState();
        policyState.markFullScanRequested();
        final SplitServiceImpl<String, String> runtime =
                new SplitServiceImpl<>(
                        conf, runtimeTuningState, synchronizedKeyToSegmentMap,
                        segmentRegistry, splitExecutionCoordinator,
                        directExecutor(), splitPolicyScheduler,
                        state::get,
                        SplitFailureReporter.noOp(),
                        SplitTelemetry.from(new Stats()), policyState);

        runtime.awaitQuiescence();

        assertFalse(policyState.isFullScanRequested());
        verify(splitExecutionCoordinator).awaitSplitsIdle(50);
    }

    @Test
    void createRejectsNullConfiguration() {
        final RuntimeTuningState tuningState = mock(RuntimeTuningState.class);
        final KeyToSegmentMap<String> map = mock(KeyToSegmentMap.class);
        final SegmentRegistry<String, String> registry = mock(
                SegmentRegistry.class);
        final SplitExecutionCoordinator<String, String> executionCoordinator =
                mock(SplitExecutionCoordinator.class);
        final ScheduledExecutorService scheduler = mock(
                ScheduledExecutorService.class);
        final Supplier<SegmentIndexState> stateSupplier =
                () -> SegmentIndexState.READY;
        final SplitFailureReporter failureReporter = SplitFailureReporter.noOp();
        final SplitTelemetry telemetry = SplitTelemetry.noOp();
        final SplitPolicyState policyState = new SplitPolicyState();

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SplitServiceImpl<>(null, tuningState, map, registry,
                        executionCoordinator, directExecutor(), scheduler,
                        stateSupplier, failureReporter, telemetry, policyState));

        assertEquals("Property 'conf' must not be null.", ex.getMessage());
    }

    @Test
    void requestFullSplitScan_requestsFullScanWhenPolicyEnabled() {
        when(conf.isBackgroundMaintenanceAutoEnabled()).thenReturn(Boolean.TRUE);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                        .thenReturn(mock(ScheduledFuture.class));
        final SplitPolicyState policyState =
                new SplitPolicyState();
        final Executor workerExecutor = mock(Executor.class);
        final SplitServiceImpl<String, String> runtime =
                new SplitServiceImpl<>(
                        conf, runtimeTuningState, synchronizedKeyToSegmentMap,
                        segmentRegistry, splitExecutionCoordinator,
                        workerExecutor, splitPolicyScheduler,
                        () -> SegmentIndexState.READY,
                        SplitFailureReporter.noOp(),
                        SplitTelemetry.from(new Stats()), policyState);

        runtime.requestFullSplitScan();

        assertTrue(policyState.isFullScanRequested());
        verify(workerExecutor).execute(any(Runnable.class));
        verify(splitPolicyScheduler).schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void requestFullSplitScan_drainsHintedAndFullScanWorkOnWorker() {
        when(conf.isBackgroundMaintenanceAutoEnabled()).thenReturn(Boolean.TRUE);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                        .thenReturn(mock(ScheduledFuture.class));
        when(runtimeTuningState.effectiveValue(any())).thenReturn(0);
        final SplitPolicyState policyState =
                new SplitPolicyState();
        final SplitServiceImpl<String, String> runtime =
                new SplitServiceImpl<>(
                        conf, runtimeTuningState, synchronizedKeyToSegmentMap,
                        segmentRegistry, splitExecutionCoordinator,
                        directExecutor(), splitPolicyScheduler,
                        () -> SegmentIndexState.READY,
                        SplitFailureReporter.noOp(),
                        SplitTelemetry.from(new Stats()), policyState);

        runtime.hintSplitCandidate(SegmentId.of(1));
        runtime.requestFullSplitScan();

        assertFalse(policyState.isWorkerActive());
        assertFalse(policyState.isFullScanRequested());
    }

    @Test
    void hintAndFullSplitScanDeduplicateSegmentBeforeWorkerRuns() {
        final SegmentId segmentId = SegmentId.of(13);
        final BlockingSegment.Runtime segmentRuntime =
                mock(BlockingSegment.Runtime.class);
        final List<Runnable> scheduledWorkers = new ArrayList<>();
        when(conf.isBackgroundMaintenanceAutoEnabled()).thenReturn(Boolean.TRUE);
        when(conf.getNumberOfIndexMaintenanceThreads()).thenReturn(1);
        when(conf.getIndexBusyBackoffMillis()).thenReturn(1);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                        .thenReturn(mock(ScheduledFuture.class));
        when(runtimeTuningState.effectiveValue(any())).thenReturn(10);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId),
                List.of(segmentId), List.of(segmentId));
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.getRuntime()).thenReturn(segmentRuntime);
        when(segmentRuntime.getNumberOfKeysInCache()).thenReturn(11L);
        final SplitServiceImpl<String, String> runtime =
                new SplitServiceImpl<>(
                        conf, runtimeTuningState, synchronizedKeyToSegmentMap,
                        segmentRegistry, splitExecutionCoordinator,
                        scheduledWorkers::add, splitPolicyScheduler,
                        () -> SegmentIndexState.READY,
                        SplitFailureReporter.noOp(),
                        SplitTelemetry.from(new Stats()),
                        new SplitPolicyState());

        runtime.hintSplitCandidate(segmentId);
        runtime.requestFullSplitScan();
        assertEquals(1, scheduledWorkers.size());

        scheduledWorkers.get(0).run();

        verify(splitExecutionCoordinator).scheduleEligibleSplit(segmentHandle,
                10, 11L);
    }

    @Test
    void hintSplitCandidateDoesNotScheduleSplitWhenThresholdDisabled() {
        when(conf.isBackgroundMaintenanceAutoEnabled()).thenReturn(Boolean.TRUE);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                        .thenReturn(mock(ScheduledFuture.class));
        when(runtimeTuningState.effectiveValue(any())).thenReturn(0);
        final SplitServiceImpl<String, String> runtime =
                new SplitServiceImpl<>(
                        conf, runtimeTuningState, synchronizedKeyToSegmentMap,
                        segmentRegistry, splitExecutionCoordinator,
                        directExecutor(), splitPolicyScheduler,
                        () -> SegmentIndexState.READY,
                        SplitFailureReporter.noOp(),
                        SplitTelemetry.from(new Stats()),
                        new SplitPolicyState());

        runtime.hintSplitCandidate(SegmentId.of(7));

        verify(splitExecutionCoordinator, never()).scheduleEligibleSplit(any(),
                any(Integer.class), any(Long.class));
    }

    @Test
    void requestFullSplitScan_schedulesEligibleMappedSegments() {
        final SegmentId segmentId = SegmentId.of(3);
        final BlockingSegment.Runtime segmentRuntime =
                mock(BlockingSegment.Runtime.class);
        when(conf.isBackgroundMaintenanceAutoEnabled()).thenReturn(Boolean.TRUE);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                        .thenReturn(mock(ScheduledFuture.class));
        when(runtimeTuningState.effectiveValue(any())).thenReturn(10);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId),
                List.of(segmentId));
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.getRuntime()).thenReturn(segmentRuntime);
        when(segmentRuntime.getNumberOfKeysInCache()).thenReturn(11L);
        when(splitExecutionCoordinator.scheduleEligibleSplit(segmentHandle, 10,
                11L)).thenReturn(true);
        final SplitServiceImpl<String, String> runtime =
                new SplitServiceImpl<>(
                        conf, runtimeTuningState, synchronizedKeyToSegmentMap,
                        segmentRegistry, splitExecutionCoordinator,
                        directExecutor(), splitPolicyScheduler,
                        () -> SegmentIndexState.READY,
                        SplitFailureReporter.noOp(),
                        SplitTelemetry.from(new Stats()),
                        new SplitPolicyState());

        runtime.requestFullSplitScan();

        verify(splitExecutionCoordinator).scheduleEligibleSplit(segmentHandle,
                10, 11L);
    }

    @Test
    void requestFullSplitScan_skipsUnloadedCandidates() {
        final SegmentId segmentId = SegmentId.of(9);
        when(conf.isBackgroundMaintenanceAutoEnabled()).thenReturn(Boolean.TRUE);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                        .thenReturn(mock(ScheduledFuture.class));
        when(runtimeTuningState.effectiveValue(any())).thenReturn(10);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId),
                List.of(segmentId));
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.empty());
        final SplitServiceImpl<String, String> runtime =
                new SplitServiceImpl<>(
                        conf, runtimeTuningState, synchronizedKeyToSegmentMap,
                        segmentRegistry, splitExecutionCoordinator,
                        directExecutor(), splitPolicyScheduler,
                        () -> SegmentIndexState.READY,
                        SplitFailureReporter.noOp(),
                        SplitTelemetry.from(new Stats()),
                        new SplitPolicyState());

        runtime.requestFullSplitScan();

        verify(segmentRegistry).tryGetSegment(segmentId);
        verify(splitExecutionCoordinator, never()).scheduleEligibleSplit(any(),
                any(Integer.class), any(Long.class));
    }

    @Test
    void requestFullSplitScan_skipsCandidatesBelowThreshold() {
        final SegmentId segmentId = SegmentId.of(11);
        final BlockingSegment.Runtime runtime = mock(BlockingSegment.Runtime.class);
        when(conf.isBackgroundMaintenanceAutoEnabled()).thenReturn(Boolean.TRUE);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                        .thenReturn(mock(ScheduledFuture.class));
        when(runtimeTuningState.effectiveValue(any())).thenReturn(10);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.getRuntime()).thenReturn(runtime);
        when(runtime.getNumberOfKeysInCache()).thenReturn(9L);
        final SplitServiceImpl<String, String> runtimeUnderTest =
                new SplitServiceImpl<>(
                        conf, runtimeTuningState, synchronizedKeyToSegmentMap,
                        segmentRegistry, splitExecutionCoordinator,
                        directExecutor(), splitPolicyScheduler,
                        () -> SegmentIndexState.READY,
                        SplitFailureReporter.noOp(),
                        SplitTelemetry.from(new Stats()),
                        new SplitPolicyState());

        runtimeUnderTest.requestFullSplitScan();

        verify(segmentRegistry).tryGetSegment(segmentId);
        verify(splitExecutionCoordinator, never()).scheduleEligibleSplit(any(),
                any(Integer.class), any(Long.class));
    }

    @Test
    void closeIsIdempotent() {
        when(conf.getIndexBusyTimeoutMillis()).thenReturn(25);
        when(splitExecutionCoordinator.splitInFlightCount()).thenReturn(0);
        final SplitServiceImpl<String, String> runtime =
                new SplitServiceImpl<>(
                        conf, runtimeTuningState, synchronizedKeyToSegmentMap,
                        segmentRegistry, splitExecutionCoordinator,
                        directExecutor(), splitPolicyScheduler,
                        () -> SegmentIndexState.READY,
                        SplitFailureReporter.noOp(),
                        SplitTelemetry.from(new Stats()),
                        new SplitPolicyState());

        runtime.close();
        runtime.close();

        verify(splitExecutionCoordinator).awaitSplitsIdle(25);
    }

    @Test
    void hintSplitCandidateRejectsCallsAfterClose() {
        final SegmentId segmentId = SegmentId.of(4);
        when(conf.getIndexBusyTimeoutMillis()).thenReturn(25);
        when(splitExecutionCoordinator.splitInFlightCount()).thenReturn(0);
        final SplitServiceImpl<String, String> runtime =
                new SplitServiceImpl<>(
                        conf, runtimeTuningState, synchronizedKeyToSegmentMap,
                        segmentRegistry, splitExecutionCoordinator,
                        directExecutor(), splitPolicyScheduler,
                        () -> SegmentIndexState.READY,
                        SplitFailureReporter.noOp(),
                        SplitTelemetry.from(new Stats()),
                        new SplitPolicyState());

        runtime.close();

        assertThrows(IllegalStateException.class,
                () -> runtime.hintSplitCandidate(segmentId));
    }

    @Test
    void awaitQuiescenceWaitsForRunningPolicyWorker() throws Exception {
        final SegmentId segmentId = SegmentId.of(21);
        final BlockingSegment.Runtime segmentRuntime =
                mock(BlockingSegment.Runtime.class);
        final CountDownLatch splitEntered = new CountDownLatch(1);
        final CountDownLatch releaseSplit = new CountDownLatch(1);
        final CountDownLatch quiescenceReturned = new CountDownLatch(1);
        final ExecutorService workerExecutor = Executors
                .newSingleThreadExecutor();
        final ExecutorService waitExecutor = Executors.newSingleThreadExecutor();
        when(conf.isBackgroundMaintenanceAutoEnabled()).thenReturn(Boolean.TRUE);
        when(conf.getIndexBusyTimeoutMillis()).thenReturn(1_000);
        when(conf.getIndexBusyBackoffMillis()).thenReturn(1);
        when(conf.getNumberOfIndexMaintenanceThreads()).thenReturn(1);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                        .thenReturn(mock(ScheduledFuture.class));
        when(runtimeTuningState.effectiveValue(any())).thenReturn(10);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId),
                List.of(segmentId));
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.getRuntime()).thenReturn(segmentRuntime);
        when(segmentRuntime.getNumberOfKeysInCache()).thenReturn(11L);
        when(splitExecutionCoordinator.scheduleEligibleSplit(segmentHandle, 10,
                11L))
                        .thenAnswer(invocation -> {
                            splitEntered.countDown();
                            assertTrue(releaseSplit.await(1,
                                    TimeUnit.SECONDS));
                            return true;
                        });
        final SplitServiceImpl<String, String> runtime =
                new SplitServiceImpl<>(
                        conf, runtimeTuningState, synchronizedKeyToSegmentMap,
                        segmentRegistry, splitExecutionCoordinator,
                        workerExecutor, splitPolicyScheduler,
                        () -> SegmentIndexState.READY,
                        SplitFailureReporter.noOp(),
                        SplitTelemetry.from(new Stats()),
                        new SplitPolicyState());
        try {
            runtime.hintSplitCandidate(segmentId);
            assertTrue(splitEntered.await(1, TimeUnit.SECONDS));

            final Future<?> quiescenceFuture = waitExecutor.submit(() -> {
                runtime.awaitQuiescence();
                quiescenceReturned.countDown();
            });

            assertFalse(quiescenceReturned.await(100, TimeUnit.MILLISECONDS));
            releaseSplit.countDown();
            assertTrue(quiescenceReturned.await(1, TimeUnit.SECONDS));
            quiescenceFuture.get(1, TimeUnit.SECONDS);
        } finally {
            workerExecutor.shutdownNow();
            waitExecutor.shutdownNow();
            assertTrue(workerExecutor.awaitTermination(1, TimeUnit.SECONDS));
            assertTrue(waitExecutor.awaitTermination(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void closeWaitsForRunningPolicyWorkerToDrain() throws Exception {
        final SegmentId segmentId = SegmentId.of(22);
        final BlockingSegment.Runtime segmentRuntime =
                mock(BlockingSegment.Runtime.class);
        final CountDownLatch splitEntered = new CountDownLatch(1);
        final CountDownLatch releaseSplit = new CountDownLatch(1);
        final CountDownLatch closeReturned = new CountDownLatch(1);
        final ExecutorService workerExecutor = Executors
                .newSingleThreadExecutor();
        final ExecutorService closeExecutor = Executors.newSingleThreadExecutor();
        when(conf.isBackgroundMaintenanceAutoEnabled()).thenReturn(Boolean.TRUE);
        when(conf.getIndexBusyTimeoutMillis()).thenReturn(1_000);
        when(conf.getIndexBusyBackoffMillis()).thenReturn(1);
        when(conf.getNumberOfIndexMaintenanceThreads()).thenReturn(1);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                        .thenReturn(mock(ScheduledFuture.class));
        when(runtimeTuningState.effectiveValue(any())).thenReturn(10);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId),
                List.of(segmentId));
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.getRuntime()).thenReturn(segmentRuntime);
        when(segmentRuntime.getNumberOfKeysInCache()).thenReturn(11L);
        when(splitExecutionCoordinator.scheduleEligibleSplit(segmentHandle, 10,
                11L))
                        .thenAnswer(invocation -> {
                            splitEntered.countDown();
                            assertTrue(releaseSplit.await(1,
                                    TimeUnit.SECONDS));
                            return true;
                        });
        final SplitServiceImpl<String, String> runtime =
                new SplitServiceImpl<>(
                        conf, runtimeTuningState, synchronizedKeyToSegmentMap,
                        segmentRegistry, splitExecutionCoordinator,
                        workerExecutor, splitPolicyScheduler,
                        () -> SegmentIndexState.READY,
                        SplitFailureReporter.noOp(),
                        SplitTelemetry.from(new Stats()),
                        new SplitPolicyState());
        try {
            runtime.hintSplitCandidate(segmentId);
            assertTrue(splitEntered.await(1, TimeUnit.SECONDS));

            final Future<?> closeFuture = closeExecutor.submit(() -> {
                runtime.close();
                closeReturned.countDown();
            });

            assertFalse(closeReturned.await(100, TimeUnit.MILLISECONDS));
            releaseSplit.countDown();
            assertTrue(closeReturned.await(1, TimeUnit.SECONDS));
            closeFuture.get(1, TimeUnit.SECONDS);
        } finally {
            workerExecutor.shutdownNow();
            closeExecutor.shutdownNow();
            assertTrue(workerExecutor.awaitTermination(1, TimeUnit.SECONDS));
            assertTrue(closeExecutor.awaitTermination(1, TimeUnit.SECONDS));
        }
    }

    private Executor directExecutor() {
        return Runnable::run;
    }
}
