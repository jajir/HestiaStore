package org.hestiastore.index.segmentindex.core.split;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
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
import org.hestiastore.index.segmentindex.IndexMaintenanceConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.metrics.Stats;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapImpl;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SplitServiceImplTest {

    @Mock
    private IndexConfiguration<String, String> conf;

    @Mock
    private IndexMaintenanceConfiguration maintenance;

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
        lenient().when(conf.maintenance()).thenReturn(maintenance);
        lenient().when(maintenance.busyTimeoutMillis()).thenReturn(50);
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
    }

    @Test
    void awaitQuiescenceClearsPendingRequestsWhenPolicyDisabled() {
        when(conf.maintenance().backgroundAutoEnabled()).thenReturn(Boolean.TRUE);
        when(conf.maintenance().busyTimeoutMillis()).thenReturn(50);
        when(splitExecutionCoordinator.splitInFlightCount()).thenReturn(0);
        final AtomicReference<SegmentIndexState> state = new AtomicReference<>(
                SegmentIndexState.CLOSING);
        final SplitPolicyState policyState = new SplitPolicyState();
        policyState.markFullScanRequested();
        final SplitServiceImpl<String, String> runtime = newRuntime(
                directExecutor(), state::get, policyState);

        runtime.awaitQuiescence();

        assertFalse(policyState.isFullScanRequested());
        verify(splitExecutionCoordinator).awaitSplitsIdle(50);
        runtime.close();
    }

    @Test
    void createRejectsNullConfiguration() {
        final RuntimeTuningState tuningState = mock(RuntimeTuningState.class);
        final KeyToSegmentMap<String> map = mockKeyToSegmentMap();
        final SegmentRegistry<String, String> registry = mockSegmentRegistry();
        final ScheduledExecutorService scheduler = mock(
                ScheduledExecutorService.class);
        final Supplier<SegmentIndexState> stateSupplier = () -> SegmentIndexState.READY;

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> SplitService.<String, String>builder()
                        .conf(null)
                        .runtimeTuningState(tuningState)
                        .keyToSegmentMap(map)
                        .segmentTopology(mockSegmentTopology())
                        .segmentRegistry(registry)
                        .directoryFacade(mock(Directory.class))
                        .splitExecutor(directExecutor())
                        .workerExecutor(directExecutor())
                        .splitPolicyScheduler(scheduler)
                        .stateSupplier(stateSupplier)
                        .failureHandler(failure -> {
                        })
                        .stats(new Stats())
                        .build());

        assertEquals("Property 'conf' must not be null.", ex.getMessage());
    }

    @Test
    void requestFullSplitScan_requestsFullScanWhenPolicyEnabled() {
        when(conf.maintenance().backgroundAutoEnabled()).thenReturn(Boolean.TRUE);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                .thenReturn(mockScheduledFuture());
        final SplitPolicyState policyState = new SplitPolicyState();
        final Executor workerExecutor = mock(Executor.class);
        final SplitServiceImpl<String, String> runtime = newRuntime(
                workerExecutor, () -> SegmentIndexState.READY, policyState);

        runtime.requestFullSplitScan();

        assertTrue(policyState.isFullScanRequested());
        final ArgumentCaptor<Runnable> worker = ArgumentCaptor
                .forClass(Runnable.class);
        verify(workerExecutor).execute(worker.capture());
        verify(splitPolicyScheduler).schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS));
        worker.getValue().run();
        runtime.close();
    }

    @Test
    void requestFullSplitScan_drainsHintedAndFullScanWorkOnWorker() {
        when(conf.maintenance().backgroundAutoEnabled()).thenReturn(Boolean.TRUE);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                .thenReturn(mockScheduledFuture());
        when(runtimeTuningState.effectiveValue(any())).thenReturn(0);
        final SplitPolicyState policyState = new SplitPolicyState();
        final SplitServiceImpl<String, String> runtime = newRuntime(
                directExecutor(), () -> SegmentIndexState.READY, policyState);

        runtime.hintSplitCandidate(SegmentId.of(1));
        runtime.requestFullSplitScan();

        assertFalse(policyState.isWorkerActive());
        assertFalse(policyState.isFullScanRequested());
        runtime.close();
    }

    @Test
    void hintAndFullSplitScanDeduplicateSegmentBeforeWorkerRuns() {
        final SegmentId segmentId = SegmentId.of(13);
        final BlockingSegment.Runtime segmentRuntime = mock(BlockingSegment.Runtime.class);
        final List<Runnable> scheduledWorkers = new ArrayList<>();
        when(conf.maintenance().backgroundAutoEnabled()).thenReturn(Boolean.TRUE);
        when(conf.maintenance().indexThreads()).thenReturn(1);
        when(conf.maintenance().busyBackoffMillis()).thenReturn(1);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                .thenReturn(mockScheduledFuture());
        when(runtimeTuningState.effectiveValue(any())).thenReturn(10);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId))
                .thenReturn(List.of(segmentId)).thenReturn(List.of(segmentId));
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.getRuntime()).thenReturn(segmentRuntime);
        when(segmentRuntime.getNumberOfKeysInCache()).thenReturn(11L);
        final SplitServiceImpl<String, String> runtime = newRuntime(
                scheduledWorkers::add, () -> SegmentIndexState.READY,
                new SplitPolicyState());

        runtime.hintSplitCandidate(segmentId);
        runtime.requestFullSplitScan();
        assertEquals(1, scheduledWorkers.size());

        scheduledWorkers.get(0).run();

        verify(splitExecutionCoordinator).scheduleEligibleSplit(segmentHandle,
                10, 11L);
        runtime.close();
    }

    @Test
    void hintSplitCandidateDoesNotScheduleSplitWhenThresholdDisabled() {
        when(conf.maintenance().backgroundAutoEnabled()).thenReturn(Boolean.TRUE);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                .thenReturn(mockScheduledFuture());
        when(runtimeTuningState.effectiveValue(any())).thenReturn(0);
        final SplitServiceImpl<String, String> runtime = newRuntime(
                directExecutor(), () -> SegmentIndexState.READY,
                new SplitPolicyState());

        runtime.hintSplitCandidate(SegmentId.of(7));

        verify(splitExecutionCoordinator, never()).scheduleEligibleSplit(any(),
                any(Integer.class), any(Long.class));
        runtime.close();
    }

    @Test
    void requestFullSplitScan_schedulesEligibleMappedSegments() {
        final SegmentId segmentId = SegmentId.of(3);
        final BlockingSegment.Runtime segmentRuntime = mock(BlockingSegment.Runtime.class);
        when(conf.maintenance().backgroundAutoEnabled()).thenReturn(Boolean.TRUE);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                .thenReturn(mockScheduledFuture());
        when(runtimeTuningState.effectiveValue(any())).thenReturn(10);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId))
                .thenReturn(List.of(segmentId));
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.getRuntime()).thenReturn(segmentRuntime);
        when(segmentRuntime.getNumberOfKeysInCache()).thenReturn(11L);
        when(splitExecutionCoordinator.scheduleEligibleSplit(segmentHandle, 10,
                11L)).thenReturn(true);
        final SplitServiceImpl<String, String> runtime = newRuntime(
                directExecutor(), () -> SegmentIndexState.READY,
                new SplitPolicyState());

        runtime.requestFullSplitScan();

        verify(splitExecutionCoordinator).scheduleEligibleSplit(segmentHandle,
                10, 11L);
        runtime.close();
    }

    @Test
    void requestFullSplitScan_skipsUnloadedCandidates() {
        final SegmentId segmentId = SegmentId.of(9);
        when(conf.maintenance().backgroundAutoEnabled()).thenReturn(Boolean.TRUE);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                .thenReturn(mockScheduledFuture());
        when(runtimeTuningState.effectiveValue(any())).thenReturn(10);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId))
                .thenReturn(List.of(segmentId));
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.empty());
        final SplitServiceImpl<String, String> runtime = newRuntime(
                directExecutor(), () -> SegmentIndexState.READY,
                new SplitPolicyState());

        runtime.requestFullSplitScan();

        verify(segmentRegistry).tryGetSegment(segmentId);
        verify(splitExecutionCoordinator, never()).scheduleEligibleSplit(any(),
                any(Integer.class), any(Long.class));
        runtime.close();
    }

    @Test
    void requestFullSplitScan_skipsCandidatesBelowThreshold() {
        final SegmentId segmentId = SegmentId.of(11);
        final BlockingSegment.Runtime runtime = mock(BlockingSegment.Runtime.class);
        when(conf.maintenance().backgroundAutoEnabled()).thenReturn(Boolean.TRUE);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                .thenReturn(mockScheduledFuture());
        when(runtimeTuningState.effectiveValue(any())).thenReturn(10);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(segmentHandle.getRuntime()).thenReturn(runtime);
        when(runtime.getNumberOfKeysInCache()).thenReturn(9L);
        final SplitServiceImpl<String, String> runtimeUnderTest = newRuntime(
                directExecutor(), () -> SegmentIndexState.READY,
                new SplitPolicyState());

        runtimeUnderTest.requestFullSplitScan();

        verify(segmentRegistry).tryGetSegment(segmentId);
        verify(splitExecutionCoordinator, never()).scheduleEligibleSplit(any(),
                any(Integer.class), any(Long.class));
        runtimeUnderTest.close();
    }

    @Test
    void closeIsIdempotent() {
        when(conf.maintenance().busyTimeoutMillis()).thenReturn(25);
        when(splitExecutionCoordinator.splitInFlightCount()).thenReturn(0);
        final SplitServiceImpl<String, String> runtime = newRuntime(
                directExecutor(), () -> SegmentIndexState.READY,
                new SplitPolicyState());

        runtime.close();
        runtime.close();

        verify(splitExecutionCoordinator).awaitSplitsIdle(25);
    }

    @Test
    void hintSplitCandidateRejectsCallsAfterClose() {
        final SegmentId segmentId = SegmentId.of(4);
        when(conf.maintenance().busyTimeoutMillis()).thenReturn(25);
        when(splitExecutionCoordinator.splitInFlightCount()).thenReturn(0);
        final SplitServiceImpl<String, String> runtime = newRuntime(
                directExecutor(), () -> SegmentIndexState.READY,
                new SplitPolicyState());

        runtime.close();

        assertThrows(IllegalStateException.class,
                () -> runtime.hintSplitCandidate(segmentId));
    }

    @Test
    void awaitQuiescenceWaitsForRunningPolicyWorker() throws Exception {
        final SegmentId segmentId = SegmentId.of(21);
        final BlockingSegment.Runtime segmentRuntime = mock(BlockingSegment.Runtime.class);
        final CountDownLatch splitEntered = new CountDownLatch(1);
        final CountDownLatch releaseSplit = new CountDownLatch(1);
        final CountDownLatch quiescenceReturned = new CountDownLatch(1);
        final ExecutorService workerExecutor = Executors
                .newSingleThreadExecutor();
        final ExecutorService waitExecutor = Executors.newSingleThreadExecutor();
        when(conf.maintenance().backgroundAutoEnabled()).thenReturn(Boolean.TRUE);
        when(conf.maintenance().busyTimeoutMillis()).thenReturn(1_000);
        when(conf.maintenance().busyBackoffMillis()).thenReturn(1);
        when(conf.maintenance().indexThreads()).thenReturn(1);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                .thenReturn(mockScheduledFuture());
        when(runtimeTuningState.effectiveValue(any())).thenReturn(10);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId))
                .thenReturn(List.of(segmentId));
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
        final SplitServiceImpl<String, String> runtime = newRuntime(
                workerExecutor, () -> SegmentIndexState.READY,
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
            releaseSplit.countDown();
            runtime.close();
            workerExecutor.shutdownNow();
            waitExecutor.shutdownNow();
            assertTrue(workerExecutor.awaitTermination(1, TimeUnit.SECONDS));
            assertTrue(waitExecutor.awaitTermination(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void closeWaitsForRunningPolicyWorkerToDrain() throws Exception {
        final SegmentId segmentId = SegmentId.of(22);
        final BlockingSegment.Runtime segmentRuntime = mock(BlockingSegment.Runtime.class);
        final CountDownLatch splitEntered = new CountDownLatch(1);
        final CountDownLatch releaseSplit = new CountDownLatch(1);
        final CountDownLatch closeReturned = new CountDownLatch(1);
        final ExecutorService workerExecutor = Executors
                .newSingleThreadExecutor();
        final ExecutorService closeExecutor = Executors.newSingleThreadExecutor();
        when(conf.maintenance().backgroundAutoEnabled()).thenReturn(Boolean.TRUE);
        when(conf.maintenance().busyTimeoutMillis()).thenReturn(1_000);
        when(conf.maintenance().busyBackoffMillis()).thenReturn(1);
        when(conf.maintenance().indexThreads()).thenReturn(1);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                .thenReturn(mockScheduledFuture());
        when(runtimeTuningState.effectiveValue(any())).thenReturn(10);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId))
                .thenReturn(List.of(segmentId));
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
        final SplitServiceImpl<String, String> runtime = newRuntime(
                workerExecutor, () -> SegmentIndexState.READY,
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

    private SplitServiceImpl<String, String> newRuntime(
            final Executor workerExecutor,
            final Supplier<SegmentIndexState> stateSupplier,
            final SplitPolicyState policyState) {
        final ManagedSplitRuntimeState managedState = new ManagedSplitRuntimeState();
        managedState.markRunning();
        return new SplitServiceImpl<>(splitExecutionCoordinator,
                new SplitPolicyCoordinator<>(conf, runtimeTuningState,
                        synchronizedKeyToSegmentMap, segmentRegistry,
                        splitExecutionCoordinator, workerExecutor,
                        splitPolicyScheduler, stateSupplier,
                        SplitFailureReporter.noOp(),
                        SplitTelemetry.from(new Stats()), policyState,
                        new SplitCandidateRegistry()),
                managedState);
    }

    @SuppressWarnings("unchecked")
    private static <V> ScheduledFuture<V> mockScheduledFuture() {
        return mock(ScheduledFuture.class);
    }

    @SuppressWarnings("unchecked")
    private static <K> KeyToSegmentMap<K> mockKeyToSegmentMap() {
        return mock(KeyToSegmentMap.class);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> SegmentRegistry<K, V> mockSegmentRegistry() {
        return mock(SegmentRegistry.class);
    }

    @SuppressWarnings("unchecked")
    private static <K> SegmentTopology<K> mockSegmentTopology() {
        return mock(SegmentTopology.class);
    }

}
