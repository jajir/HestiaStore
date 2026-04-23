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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapImpl;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentregistry.SegmentHandle;
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
    private BackgroundSplitCoordinator<String, String> backgroundSplitCoordinator;

    @Mock
    private SegmentHandle<String, String> segmentHandle;

    @Mock
    private ScheduledExecutorService splitPolicyScheduler;

    @BeforeEach
    void setUp() {
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
    }

    @Test
    void awaitExhaustedClearsPendingRequestsWhenPolicyDisabled() {
        when(conf.isBackgroundMaintenanceAutoEnabled()).thenReturn(Boolean.TRUE);
        when(conf.getIndexBusyTimeoutMillis()).thenReturn(50);
        when(backgroundSplitCoordinator.splitInFlightCount()).thenReturn(0);
        final AtomicReference<SegmentIndexState> state = new AtomicReference<>(
                SegmentIndexState.CLOSING);
        final BackgroundSplitPolicyWorkState workState =
                new BackgroundSplitPolicyWorkState();
        workState.markScanRequested();
        workState.addHint(SegmentId.of(7));
        final SplitServiceImpl<String, String> runtime =
                new SplitServiceImpl<>(
                        conf, runtimeTuningState, synchronizedKeyToSegmentMap,
                        segmentRegistry, backgroundSplitCoordinator,
                        directExecutor(), splitPolicyScheduler,
                        SplitRuntimeLifecycle.from(state::get),
                        SplitFailureReporter.noOp(),
                        SplitRuntimeTelemetry.from(new Stats()), workState);

        runtime.awaitExhausted();

        assertFalse(workState.isScanRequested());
        assertFalse(workState.hasPendingHints());
        verify(backgroundSplitCoordinator).awaitSplitsIdle(50);
    }

    @Test
    void createRejectsNullConfiguration() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SplitServiceImpl<>(null,
                        mock(RuntimeTuningState.class),
                        mock(KeyToSegmentMap.class), mock(SegmentRegistry.class),
                        mock(BackgroundSplitCoordinator.class),
                        directExecutor(), mock(ScheduledExecutorService.class),
                        SplitRuntimeLifecycle
                                .from(() -> SegmentIndexState.READY),
                        SplitFailureReporter.noOp(),
                        SplitRuntimeTelemetry.noOp(),
                        new BackgroundSplitPolicyWorkState()));

        assertEquals("Property 'conf' must not be null.", ex.getMessage());
    }

    @Test
    void scheduleScan_requestsFullScanWhenPolicyEnabled() {
        when(conf.isBackgroundMaintenanceAutoEnabled()).thenReturn(Boolean.TRUE);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                        .thenReturn(mock(ScheduledFuture.class));
        final BackgroundSplitPolicyWorkState workState =
                new BackgroundSplitPolicyWorkState();
        final Executor workerExecutor = mock(Executor.class);
        final SplitServiceImpl<String, String> runtime =
                new SplitServiceImpl<>(
                        conf, runtimeTuningState, synchronizedKeyToSegmentMap,
                        segmentRegistry, backgroundSplitCoordinator,
                        workerExecutor, splitPolicyScheduler,
                        SplitRuntimeLifecycle
                                .from(() -> SegmentIndexState.READY),
                        SplitFailureReporter.noOp(),
                        SplitRuntimeTelemetry.from(new Stats()), workState);

        runtime.scheduleScan();

        assertTrue(workState.isScanRequested());
        verify(workerExecutor).execute(any(Runnable.class));
        verify(splitPolicyScheduler).schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void scheduleScanIfIdle_skipsRequestWhenSplitCoordinatorBusy() {
        when(conf.isBackgroundMaintenanceAutoEnabled()).thenReturn(Boolean.TRUE);
        when(backgroundSplitCoordinator.splitInFlightCount()).thenReturn(1);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                        .thenReturn(mock(ScheduledFuture.class));
        final BackgroundSplitPolicyWorkState workState =
                new BackgroundSplitPolicyWorkState();
        final Executor workerExecutor = mock(Executor.class);
        final SplitServiceImpl<String, String> runtime =
                new SplitServiceImpl<>(
                        conf, runtimeTuningState, synchronizedKeyToSegmentMap,
                        segmentRegistry, backgroundSplitCoordinator,
                        workerExecutor, splitPolicyScheduler,
                        SplitRuntimeLifecycle
                                .from(() -> SegmentIndexState.READY),
                        SplitFailureReporter.noOp(),
                        SplitRuntimeTelemetry.from(new Stats()), workState);

        runtime.scheduleScanIfIdle();

        assertFalse(workState.isScanRequested());
    }

    @Test
    void scheduleScan_drainsHintedAndFullScanWorkOnWorker() {
        when(conf.isBackgroundMaintenanceAutoEnabled()).thenReturn(Boolean.TRUE);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                        .thenReturn(mock(ScheduledFuture.class));
        when(runtimeTuningState.effectiveValue(any())).thenReturn(0);
        final BackgroundSplitPolicyWorkState workState =
                new BackgroundSplitPolicyWorkState();
        workState.addHint(SegmentId.of(1));
        final SplitServiceImpl<String, String> runtime =
                new SplitServiceImpl<>(
                        conf, runtimeTuningState, synchronizedKeyToSegmentMap,
                        segmentRegistry, backgroundSplitCoordinator,
                        directExecutor(), splitPolicyScheduler,
                        SplitRuntimeLifecycle
                                .from(() -> SegmentIndexState.READY),
                        SplitFailureReporter.noOp(),
                        SplitRuntimeTelemetry.from(new Stats()), workState);

        runtime.scheduleScan();

        assertFalse(workState.isScanScheduled());
        assertFalse(workState.isScanRequested());
        assertFalse(workState.hasPendingHints());
    }

    @Test
    void scheduleScan_clearsHintsWhenThresholdDisabled() {
        when(conf.isBackgroundMaintenanceAutoEnabled()).thenReturn(Boolean.TRUE);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                        .thenReturn(mock(ScheduledFuture.class));
        when(runtimeTuningState.effectiveValue(any())).thenReturn(0);
        final BackgroundSplitPolicyWorkState workState =
                new BackgroundSplitPolicyWorkState();
        workState.addHint(SegmentId.of(7));
        final SplitServiceImpl<String, String> runtime =
                new SplitServiceImpl<>(
                        conf, runtimeTuningState, synchronizedKeyToSegmentMap,
                        segmentRegistry, backgroundSplitCoordinator,
                        directExecutor(), splitPolicyScheduler,
                        SplitRuntimeLifecycle
                                .from(() -> SegmentIndexState.READY),
                        SplitFailureReporter.noOp(),
                        SplitRuntimeTelemetry.from(new Stats()), workState);

        runtime.scheduleScan();

        assertFalse(workState.hasPendingHints());
    }

    @Test
    void scheduleScan_schedulesEligibleMappedSegments() {
        final SegmentId segmentId = SegmentId.of(3);
        when(conf.isBackgroundMaintenanceAutoEnabled()).thenReturn(Boolean.TRUE);
        when(splitPolicyScheduler.schedule(any(Runnable.class), eq(250L),
                eq(TimeUnit.MILLISECONDS)))
                        .thenReturn(mock(ScheduledFuture.class));
        when(runtimeTuningState.effectiveValue(any())).thenReturn(10);
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId),
                List.of(segmentId));
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));
        when(backgroundSplitCoordinator.handleSplitCandidate(segmentHandle, 10,
                false)).thenReturn(true);
        final SplitServiceImpl<String, String> runtime =
                new SplitServiceImpl<>(
                        conf, runtimeTuningState, synchronizedKeyToSegmentMap,
                        segmentRegistry, backgroundSplitCoordinator,
                        directExecutor(), splitPolicyScheduler,
                        SplitRuntimeLifecycle
                                .from(() -> SegmentIndexState.READY),
                        SplitFailureReporter.noOp(),
                        SplitRuntimeTelemetry.from(new Stats()),
                        new BackgroundSplitPolicyWorkState());

        runtime.scheduleScan();

        verify(backgroundSplitCoordinator).handleSplitCandidate(segmentHandle,
                10, false);
    }

    @Test
    void scheduleScan_skipsUnloadedCandidates() {
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
                        segmentRegistry, backgroundSplitCoordinator,
                        directExecutor(), splitPolicyScheduler,
                        SplitRuntimeLifecycle
                                .from(() -> SegmentIndexState.READY),
                        SplitFailureReporter.noOp(),
                        SplitRuntimeTelemetry.from(new Stats()),
                        new BackgroundSplitPolicyWorkState());

        runtime.scheduleScan();

        verify(segmentRegistry).tryGetSegment(segmentId);
        verify(backgroundSplitCoordinator, never()).handleSplitCandidate(any(),
                any(Integer.class), any(Boolean.class));
    }

    private Executor directExecutor() {
        return Runnable::run;
    }
}
