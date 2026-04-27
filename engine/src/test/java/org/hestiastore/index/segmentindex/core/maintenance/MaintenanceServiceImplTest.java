package org.hestiastore.index.segmentindex.core.maintenance;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationAccess;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationResult;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapImpl;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
class MaintenanceServiceImplTest {

    @Mock
    private StableSegmentOperationAccess<String, String> stableSegmentGateway;

    @Mock
    private BlockingSegment<String, String> segmentHandle;

    @Mock
    private BlockingSegment.Runtime runtime;

    @Mock
    private SplitService splitService;

    @Mock
    private Runnable checkpointAction;

    private Directory directory;
    private KeyToSegmentMapImpl<String> keyToSegmentMap;
    private KeyToSegmentMap<String> synchronizedKeyToSegmentMap;
    private MaintenanceServiceImpl<String, String> service;
    private Stats stats;
    private ExecutorService maintenanceExecutor;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        keyToSegmentMap = new KeyToSegmentMapImpl<>(directory,
                new TypeDescriptorShortString());
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
        stats = new Stats();
        maintenanceExecutor = Executors.newSingleThreadExecutor();
        service = new MaintenanceServiceImpl<>(
                LoggerFactory.getLogger(MaintenanceServiceImplTest.class),
                synchronizedKeyToSegmentMap, stableSegmentGateway,
                splitService, retryPolicy(), stats, maintenanceExecutor,
                checkpointAction);
    }

    @AfterEach
    void tearDown() {
        if (maintenanceExecutor != null) {
            maintenanceExecutor.shutdownNow();
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
                .thenReturn(StableSegmentOperationResult.busy());

        service.compact();

        verify(stableSegmentGateway, timeout(1_000)).compact(segmentId);
        assertEquals(0L, stats.getCompactBusyRetryCount());
    }

    @Test
    @SuppressWarnings("unchecked")
    void flush_flushesMappedSegmentsAndRouteMap() {
        final KeyToSegmentMap<Integer> mappedSegments = mock(
                KeyToSegmentMap.class);
        final StableSegmentOperationAccess<Integer, String> stableSegments = mock(
                StableSegmentOperationAccess.class);
        final SplitService splitServiceValue = mock(
                SplitService.class);
        final Runnable checkpoint = mock(Runnable.class);
        final SegmentId segmentId = SegmentId.of(7);
        when(mappedSegments.getSegmentIds()).thenReturn(List.of(segmentId));
        when(stableSegments.flush(segmentId)).thenReturn(StableSegmentOperationResult.busy());
        final MaintenanceServiceImpl<Integer, String> maintenance =
                new MaintenanceServiceImpl<>(
                        LoggerFactory.getLogger(
                                MaintenanceServiceImplTest.class),
                        mappedSegments, stableSegments, splitServiceValue,
                        retryPolicy(), new Stats(), maintenanceExecutor,
                        checkpoint);

        maintenance.flush();

        verify(stableSegments, timeout(1_000)).flush(segmentId);
        verify(mappedSegments, timeout(1_000)).flushIfDirty();
    }

    @Test
    void flushSegment_recordsAcceptedToReadyLatencyAndBusyRetryCount() {
        final SegmentId segmentId = createBootstrapSegment("key");
        service = new MaintenanceServiceImpl<>(
                LoggerFactory.getLogger(MaintenanceServiceImplTest.class),
                synchronizedKeyToSegmentMap, stableSegmentGateway, splitService,
                retryPolicy(), stats, maintenanceExecutor, checkpointAction,
                sequenceNanoTimeSupplier(10_000L, 35_000L));
        when(segmentHandle.getRuntime()).thenReturn(runtime);
        when(stableSegmentGateway.flush(segmentId)).thenReturn(
                StableSegmentOperationResult.busy(),
                StableSegmentOperationResult.ok(segmentHandle));
        when(runtime.getState()).thenReturn(SegmentState.READY);

        service.flushSegment(segmentId, true);

        assertEquals(1L, stats.getFlushBusyRetryCount());
        assertEquals(25L, stats.getFlushAcceptedToReadyP95Micros());
    }

    @Test
    void compactSegment_recordsAcceptedToReadyLatencyAndBusyRetryCount() {
        final SegmentId segmentId = createBootstrapSegment("key");
        service = new MaintenanceServiceImpl<>(
                LoggerFactory.getLogger(MaintenanceServiceImplTest.class),
                synchronizedKeyToSegmentMap, stableSegmentGateway, splitService,
                retryPolicy(), stats, maintenanceExecutor, checkpointAction,
                sequenceNanoTimeSupplier(20_000L, 68_000L));
        when(segmentHandle.getRuntime()).thenReturn(runtime);
        when(stableSegmentGateway.compact(segmentId)).thenReturn(
                StableSegmentOperationResult.busy(),
                StableSegmentOperationResult.ok(segmentHandle));
        when(runtime.getState()).thenReturn(SegmentState.READY);

        service.compactSegment(segmentId, true);

        assertEquals(1L, stats.getCompactBusyRetryCount());
        assertEquals(48L, stats.getCompactAcceptedToReadyP95Micros());
    }

    @Test
    void compactSegment_coalescesBusyOperationWhenWaitingIsDisabled() {
        final SegmentId segmentId = createBootstrapSegment("busy-key");
        when(stableSegmentGateway.compact(segmentId))
                .thenReturn(StableSegmentOperationResult.busy());

        assertDoesNotThrow(() -> service.compactSegment(segmentId, false));
        assertEquals(0L, stats.getCompactBusyRetryCount());
    }

    @Test
    void compactSegment_ignoresUnmappedErrorStatus() {
        final SegmentId segmentId = SegmentId.of(999);
        when(stableSegmentGateway.compact(segmentId))
                .thenReturn(StableSegmentOperationResult.error());

        assertDoesNotThrow(() -> service.compactSegment(segmentId, true));
    }

    @Test
    void flushSegment_throwsForMappedErrorStatus() {
        final SegmentId segmentId = createBootstrapSegment("error-key");
        when(stableSegmentGateway.flush(segmentId))
                .thenReturn(StableSegmentOperationResult.error());

        assertThrows(IndexException.class,
                () -> service.flushSegment(segmentId, true));
    }

    @Test
    void flushSegment_failsWhenAcceptedSegmentEntersErrorState() {
        final SegmentId segmentId = createBootstrapSegment("error-state-key");
        when(segmentHandle.getRuntime()).thenReturn(runtime);
        when(stableSegmentGateway.flush(segmentId))
                .thenReturn(StableSegmentOperationResult.ok(segmentHandle));
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
                .thenReturn(StableSegmentOperationResult.ok(segmentHandle));
        when(stableSegmentGateway.flush(secondSegment))
                .thenReturn(StableSegmentOperationResult.ok(segmentHandle));

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
                .thenReturn(StableSegmentOperationResult.ok(segmentHandle));

        service.flushAndWait();

        verify(splitService, times(2)).awaitQuiescence();
        verify(stableSegmentGateway).flush(segmentId);
        verify(checkpointAction).run();
    }

    @Test
    void compactAndWait_settlesSplitsCompactsFlushesAndCheckpoints() {
        final SegmentId segmentId = createBootstrapSegment("compact-wait-key");
        when(segmentHandle.getRuntime()).thenReturn(runtime);
        when(runtime.getState()).thenReturn(SegmentState.READY);
        when(stableSegmentGateway.compact(segmentId))
                .thenReturn(StableSegmentOperationResult.ok(segmentHandle));
        when(stableSegmentGateway.flush(segmentId))
                .thenReturn(StableSegmentOperationResult.ok(segmentHandle));

        service.compactAndWait();

        verify(splitService, times(2)).awaitQuiescence();
        verify(stableSegmentGateway).compact(segmentId);
        verify(stableSegmentGateway).flush(segmentId);
        verify(checkpointAction).run();
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

    private static IndexRetryPolicy retryPolicy() {
        return new IndexRetryPolicy(1, 1_000);
    }

    private SegmentId createBootstrapSegment(final String key) {
        synchronizedKeyToSegmentMap.extendMaxKeyIfNeeded(key);
        return synchronizedKeyToSegmentMap.findSegmentIdForKey(key);
    }
}
