package org.hestiastore.index.segmentindex.core.execution;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.core.split.SplitRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageCoordinator;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentindex.routemap.PersistentSegmentRouteMap;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MappedSegmentMaintenanceServiceBehaviorTest {

    @Mock
    private NonBlockingSegmentOperationGateway<String, String> stableSegmentGateway;

    @Mock
    private BlockingSegment<String, String> segmentHandle;

    @Mock
    private BlockingSegment.Runtime runtime;

    @Mock
    private SplitRuntime<String, String> splitService;

    @Mock
    private StorageCoordinator<String, String> storageService;

    private Directory directory;
    private PersistentSegmentRouteMap<String> keyToSegmentMap;
    private SegmentRouteMap<String> synchronizedKeyToSegmentMap;
    private MappedSegmentMaintenanceService<String, String> service;
    private MaintenanceStatsRecorder statsRecorder;
    private ExecutorService maintenanceExecutor;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        keyToSegmentMap = new PersistentSegmentRouteMap<>(directory,
                new TypeDescriptorShortString());
        synchronizedKeyToSegmentMap = keyToSegmentMap;
        statsRecorder = new MaintenanceStatsRecorder();
        maintenanceExecutor = Executors.newSingleThreadExecutor();
        service = new MappedSegmentMaintenanceService<>(
                synchronizedKeyToSegmentMap, stableSegmentGateway,
                splitService, retryPolicy(), statsRecorder, maintenanceExecutor,
                storageService, System::nanoTime);
    }

    @AfterEach
    void tearDown() {
        if (maintenanceExecutor != null) {
            maintenanceExecutor.shutdownNow();
            awaitMaintenanceExecutorClosed();
        }
        if (synchronizedKeyToSegmentMap != null
                && !synchronizedKeyToSegmentMap.wasClosed()) {
            synchronizedKeyToSegmentMap.close();
        }
    }

    @Test
    void compact_compactsMappedSegments() {
        final SegmentId segmentId = createBootstrapSegment("compact-key");
        when(stableSegmentGateway.compact(segmentId))
                .thenReturn(OperationResult.busy());

        service.compact();

        verify(stableSegmentGateway, timeout(1_000)).compact(segmentId);
        assertEquals(0L,
                statsRecorder.statsSnapshot().getCompactBusyRetryCount());
    }

    @Test
    @SuppressWarnings("unchecked")
    void flush_flushesMappedSegmentsAndRouteMap() {
        final SegmentRouteMap<Integer> mappedSegments = mock(
                SegmentRouteMap.class);
        final NonBlockingSegmentOperationGateway<Integer, String> stableSegments = mock(
                NonBlockingSegmentOperationGateway.class);
        final SplitRuntime<Integer, String> splitServiceValue = mock(
                SplitRuntime.class);
        final StorageCoordinator<Integer, String> storageServiceValue = mock(
                StorageCoordinator.class);
        final SegmentId segmentId = SegmentId.of(7);
        when(mappedSegments.getSegmentIds()).thenReturn(List.of(segmentId));
        when(stableSegments.flush(segmentId)).thenReturn(OperationResult.busy());
        final MappedSegmentMaintenanceService<Integer, String> maintenance = new MappedSegmentMaintenanceService<>(
                mappedSegments, stableSegments, splitServiceValue,
                retryPolicy(), new MaintenanceStatsRecorder(),
                maintenanceExecutor, storageServiceValue,
                System::nanoTime);

        maintenance.flush();

        verify(stableSegments, timeout(1_000)).flush(segmentId);
        verify(mappedSegments, timeout(1_000)).flushIfDirty();
    }

    @Test
    void flushSegment_recordsAcceptedToReadyLatencyAndBusyRetryCount() {
        final SegmentId segmentId = createBootstrapSegment("key");
        service = new MappedSegmentMaintenanceService<>(
                synchronizedKeyToSegmentMap, stableSegmentGateway, splitService,
                retryPolicy(), statsRecorder, maintenanceExecutor,
                storageService, sequenceNanoTimeSupplier(10_000L, 35_000L));
        when(segmentHandle.getRuntime()).thenReturn(runtime);
        when(stableSegmentGateway.flush(segmentId))
                .thenReturn(OperationResult.busy())
                .thenReturn(OperationResult.ok(segmentHandle));
        when(runtime.getState()).thenReturn(SegmentState.READY);

        service.flushSegment(segmentId, true);

        final MaintenanceStatsSnapshot stats = statsRecorder.statsSnapshot();
        assertEquals(1L, stats.getFlushBusyRetryCount());
        assertEquals(25L, stats.getFlushAcceptedToReadyP95Micros());
    }

    @Test
    void compactSegment_recordsAcceptedToReadyLatencyAndBusyRetryCount() {
        final SegmentId segmentId = createBootstrapSegment("key");
        service = new MappedSegmentMaintenanceService<>(
                synchronizedKeyToSegmentMap, stableSegmentGateway, splitService,
                retryPolicy(), statsRecorder, maintenanceExecutor,
                storageService, sequenceNanoTimeSupplier(20_000L, 68_000L));
        when(segmentHandle.getRuntime()).thenReturn(runtime);
        when(stableSegmentGateway.compact(segmentId))
                .thenReturn(OperationResult.busy())
                .thenReturn(OperationResult.ok(segmentHandle));
        when(runtime.getState()).thenReturn(SegmentState.READY);

        service.compactSegment(segmentId, true);

        final MaintenanceStatsSnapshot stats = statsRecorder.statsSnapshot();
        assertEquals(1L, stats.getCompactBusyRetryCount());
        assertEquals(48L, stats.getCompactAcceptedToReadyP95Micros());
    }

    @Test
    void compactSegment_coalescesBusyOperationWhenWaitingIsDisabled() {
        final SegmentId segmentId = createBootstrapSegment("busy-key");
        when(stableSegmentGateway.compact(segmentId))
                .thenReturn(OperationResult.busy());

        assertDoesNotThrow(() -> service.compactSegment(segmentId, false));
        assertEquals(0L,
                statsRecorder.statsSnapshot().getCompactBusyRetryCount());
    }

    @Test
    void compactSegment_ignoresUnmappedErrorStatus() {
        final SegmentId segmentId = SegmentId.of(999);
        when(stableSegmentGateway.compact(segmentId))
                .thenReturn(OperationResult.error());

        assertDoesNotThrow(() -> service.compactSegment(segmentId, true));
    }

    @Test
    void flushSegment_throwsForMappedErrorStatus() {
        final SegmentId segmentId = createBootstrapSegment("error-key");
        when(stableSegmentGateway.flush(segmentId))
                .thenReturn(OperationResult.error());

        assertThrows(IndexException.class,
                () -> service.flushSegment(segmentId, true));
    }

    @Test
    void flushSegment_failsWhenAcceptedSegmentEntersErrorState() {
        final SegmentId segmentId = createBootstrapSegment("error-state-key");
        when(segmentHandle.getRuntime()).thenReturn(runtime);
        when(stableSegmentGateway.flush(segmentId))
                .thenReturn(OperationResult.ok(segmentHandle));
        when(runtime.getState()).thenReturn(SegmentState.ERROR);

        assertThrows(IndexException.class,
                () -> service.flushSegment(segmentId, true));
    }

    @Test
    void flushMappedSegmentsAndWait_flushesMappedSegments() {
        final SegmentId firstSegment = createBootstrapSegment("key-a");
        final SegmentId secondSegment = createBootstrapSegment("key-z");
        when(segmentHandle.getRuntime()).thenReturn(runtime);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(stableSegmentGateway.flush(firstSegment))
                .thenReturn(OperationResult.ok(segmentHandle));
        when(stableSegmentGateway.flush(secondSegment))
                .thenReturn(OperationResult.ok(segmentHandle));

        service.flushMappedSegmentsAndWait();

        verify(stableSegmentGateway).flush(firstSegment);
        verify(stableSegmentGateway).flush(secondSegment);
    }

    @Test
    void flushAndWait_settlesSplitsFlushesRouteMapAndCheckpoints() {
        final SegmentId segmentId = createBootstrapSegment("wait-key");
        when(segmentHandle.getRuntime()).thenReturn(runtime);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(stableSegmentGateway.flush(segmentId))
                .thenReturn(OperationResult.ok(segmentHandle));

        service.flushAndWait();

        verify(splitService, times(2)).awaitQuiescence();
        verify(stableSegmentGateway).flush(segmentId);
        verify(storageService).checkpointWal();
    }

    @Test
    @SuppressWarnings("unchecked")
    void sealAsyncMaintenanceAndWait_drainsAcceptedTaskAndRejectsNewAsyncWork()
            throws Exception {
        final SegmentRouteMap<Integer> mappedSegments = mock(
                SegmentRouteMap.class);
        final NonBlockingSegmentOperationGateway<Integer, String> stableSegments = mock(
                NonBlockingSegmentOperationGateway.class);
        final SplitRuntime<Integer, String> splitServiceValue = mock(
                SplitRuntime.class);
        final StorageCoordinator<Integer, String> storageServiceValue = mock(
                StorageCoordinator.class);
        final MappedSegmentMaintenanceService<Integer, String> maintenance = new MappedSegmentMaintenanceService<>(
                mappedSegments, stableSegments,
                splitServiceValue, retryPolicy(),
                new MaintenanceStatsRecorder(), maintenanceExecutor,
                storageServiceValue, System::nanoTime);
        final CountDownLatch taskStarted = new CountDownLatch(1);
        final CountDownLatch releaseTask = new CountDownLatch(1);
        when(mappedSegments.getSegmentIds()).thenAnswer(invocation -> {
            taskStarted.countDown();
            awaitLatch(releaseTask);
            return List.of();
        });

        maintenance.flush();
        assertTrue(taskStarted.await(1, TimeUnit.SECONDS));
        releaseTask.countDown();

        maintenance.sealAsyncMaintenanceAndWait();

        assertThrows(IndexException.class, maintenance::flush);
    }

    @Test
    void flushAndWait_stillRunsAfterAsyncMaintenanceIsSealed() {
        final SegmentId segmentId = createBootstrapSegment("sealed-wait-key");
        when(segmentHandle.getRuntime()).thenReturn(runtime);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(stableSegmentGateway.flush(segmentId))
                .thenReturn(OperationResult.ok(segmentHandle));

        service.sealAsyncMaintenanceAndWait();
        service.flushAndWait();

        verify(stableSegmentGateway).flush(segmentId);
        verify(storageService).checkpointWal();
    }

    @Test
    @SuppressWarnings("unchecked")
    void sealAsyncMaintenanceAndWait_timesOutWhenAcceptedTaskDoesNotFinish()
            throws Exception {
        final SegmentRouteMap<Integer> mappedSegments = mock(
                SegmentRouteMap.class);
        final NonBlockingSegmentOperationGateway<Integer, String> stableSegments = mock(
                NonBlockingSegmentOperationGateway.class);
        final SplitRuntime<Integer, String> splitServiceValue = mock(
                SplitRuntime.class);
        final StorageCoordinator<Integer, String> storageServiceValue = mock(
                StorageCoordinator.class);
        final MappedSegmentMaintenanceService<Integer, String> maintenance = new MappedSegmentMaintenanceService<>(
                mappedSegments, stableSegments,
                splitServiceValue, new BusyRetryPolicy(1, 25),
                new MaintenanceStatsRecorder(), maintenanceExecutor,
                storageServiceValue, System::nanoTime);
        final CountDownLatch taskStarted = new CountDownLatch(1);
        final CountDownLatch releaseTask = new CountDownLatch(1);
        final CountDownLatch taskFinished = new CountDownLatch(1);
        when(mappedSegments.getSegmentIds()).thenAnswer(invocation -> {
            try {
                taskStarted.countDown();
                awaitLatch(releaseTask);
                return List.of();
            } finally {
                taskFinished.countDown();
            }
        });

        maintenance.flush();
        assertTrue(taskStarted.await(1, TimeUnit.SECONDS));

        try {
            assertThrows(IndexException.class,
                    maintenance::sealAsyncMaintenanceAndWait);
        } finally {
            releaseTask.countDown();
        }
        assertTrue(taskFinished.await(1, TimeUnit.SECONDS));
    }

    @Test
    void compactAndWait_settlesSplitsCompactsFlushesAndCheckpoints() {
        final SegmentId segmentId = createBootstrapSegment("compact-wait-key");
        when(segmentHandle.getRuntime()).thenReturn(runtime);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(stableSegmentGateway.compact(segmentId))
                .thenReturn(OperationResult.ok(segmentHandle));
        when(stableSegmentGateway.flush(segmentId))
                .thenReturn(OperationResult.ok(segmentHandle));

        service.compactAndWait();

        verify(splitService, times(2)).awaitQuiescence();
        verify(stableSegmentGateway).compact(segmentId);
        verify(stableSegmentGateway).flush(segmentId);
        verify(storageService).checkpointWal();
    }

    private static LongSupplier sequenceNanoTimeSupplier(
            final long... nanos) {
        final AtomicInteger index = new AtomicInteger();
        return () -> {
            final int current = index.getAndIncrement();
            final int safeIndex = Math.min(current, nanos.length - 1);
            return nanos[safeIndex];
        };
    }

    private static BusyRetryPolicy retryPolicy() {
        return new BusyRetryPolicy(1, 1_000);
    }

    private void awaitMaintenanceExecutorClosed() {
        try {
            assertTrue(maintenanceExecutor.awaitTermination(1,
                    TimeUnit.SECONDS));
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while closing maintenance executor.", e);
        }
    }

    private static void awaitLatch(final CountDownLatch latch) {
        try {
            assertTrue(latch.await(1, TimeUnit.SECONDS));
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IndexException("Interrupted while waiting for latch.", e);
        }
    }

    private SegmentId createBootstrapSegment(final String key) {
        synchronizedKeyToSegmentMap.extendMaxKeyIfNeeded(key);
        return synchronizedKeyToSegmentMap.findSegmentIdForKey(key);
    }
}
