package org.hestiastore.index.segmentindex.core.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.OperationResult;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexMaintenanceConfiguration;
import org.hestiastore.index.segmentindex.core.split.SplitRuntime;
import org.hestiastore.index.segmentindex.core.storage.StorageCoordinator;
import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.junit.jupiter.api.Test;

class MappedSegmentMaintenanceServiceTest {

    @Test
    @SuppressWarnings("unchecked")
    void builderCreatesMaintenanceService() throws InterruptedException {
        final SegmentRouteMap<Integer> keyToSegmentMap = mock(
                SegmentRouteMap.class);
        final NonBlockingSegmentOperationGateway<Integer, String> stableSegmentGateway = mock(
                NonBlockingSegmentOperationGateway.class);
        final SplitRuntime<Integer, String> splitService = mock(
                SplitRuntime.class);
        final StorageCoordinator<Integer, String> storageService = mock(
                StorageCoordinator.class);
        final ExecutorService maintenanceExecutor = Executors.newSingleThreadExecutor();
        final SegmentId segmentId = SegmentId.of(1);
        try {
            when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
            when(stableSegmentGateway.compact(segmentId))
                    .thenReturn(OperationResult.busy());
            when(stableSegmentGateway.flush(segmentId)).thenReturn(
                    OperationResult.busy());
            final MappedSegmentMaintenanceService<Integer, String> maintenance = MappedSegmentMaintenanceService.create(keyToSegmentMap,
                    stableSegmentGateway, splitService, maintenance(),
                    new MaintenanceStatsRecorder(), maintenanceExecutor,
                    storageService);

            maintenance.compact();
            maintenance.flush();
            maintenanceExecutor.shutdown();

            assertTrue(maintenanceExecutor.awaitTermination(1,
                    TimeUnit.SECONDS));
            verify(stableSegmentGateway).compact(segmentId);
            verify(stableSegmentGateway).flush(segmentId);
            verify(keyToSegmentMap).flushIfDirty();
        } finally {
            maintenanceExecutor.shutdownNow();
        }
    }

    @Test
    void createRejectsMissingKeyToSegmentMap() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> MappedSegmentMaintenanceService.create(null,
                        mockStableSegmentGateway(), mockSplitService(),
                        maintenance(), new MaintenanceStatsRecorder(),
                        mock(ExecutorService.class), mockStorageService()));

        assertEquals("Property 'keyToSegmentMap' must not be null.",
                ex.getMessage());
    }

    @Test
    @SuppressWarnings("unchecked")
    void createRejectsMissingMaintenanceExecutor() {
        final SegmentRouteMap<Integer> keyToSegmentMap = mock(
                SegmentRouteMap.class);
        final NonBlockingSegmentOperationGateway<Integer, String> stableSegmentGateway = mock(
                NonBlockingSegmentOperationGateway.class);
        final SplitRuntime<Integer, String> splitService = mock(
                SplitRuntime.class);

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> MappedSegmentMaintenanceService.create(keyToSegmentMap,
                        stableSegmentGateway, splitService, maintenance(),
                        new MaintenanceStatsRecorder(), null,
                        mockStorageService()));

        assertEquals("Property 'maintenanceExecutor' must not be null.",
                ex.getMessage());
    }

    @Test
    @SuppressWarnings("unchecked")
    void createRejectsMissingStorageService() {
        final SegmentRouteMap<Integer> keyToSegmentMap = mock(
                SegmentRouteMap.class);
        final NonBlockingSegmentOperationGateway<Integer, String> stableSegmentGateway = mock(
                NonBlockingSegmentOperationGateway.class);
        final SplitRuntime<Integer, String> splitService = mock(
                SplitRuntime.class);
        final ExecutorService maintenanceExecutor = Executors.newSingleThreadExecutor();
        try {
            final IllegalArgumentException ex = assertThrows(
                    IllegalArgumentException.class,
                    () -> MappedSegmentMaintenanceService.create(keyToSegmentMap,
                            stableSegmentGateway, splitService, maintenance(),
                            new MaintenanceStatsRecorder(), maintenanceExecutor,
                            null));

            assertEquals("Property 'storageService' must not be null.",
                    ex.getMessage());
        } finally {
            maintenanceExecutor.shutdownNow();
        }
    }

    private EffectiveIndexMaintenanceConfiguration maintenance() {
        final EffectiveIndexMaintenanceConfiguration maintenance = mock(
                EffectiveIndexMaintenanceConfiguration.class);
        when(maintenance.busyBackoffMillis()).thenReturn(1);
        when(maintenance.busyTimeoutMillis()).thenReturn(10);
        return maintenance;
    }

    @SuppressWarnings("unchecked")
    private NonBlockingSegmentOperationGateway<Integer, String> mockStableSegmentGateway() {
        return mock(NonBlockingSegmentOperationGateway.class);
    }

    @SuppressWarnings("unchecked")
    private SplitRuntime<Integer, String> mockSplitService() {
        return mock(SplitRuntime.class);
    }

    @SuppressWarnings("unchecked")
    private StorageCoordinator<Integer, String> mockStorageService() {
        return mock(StorageCoordinator.class);
    }
}
