package org.hestiastore.index.segmentindex.core.maintenance;

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
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationGateway;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.core.storage.StorageService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.junit.jupiter.api.Test;

class MaintenanceServiceTest {

    @Test
    @SuppressWarnings("unchecked")
    void builderCreatesMaintenanceService() throws InterruptedException {
        final KeyToSegmentMap<Integer> keyToSegmentMap = mock(
                KeyToSegmentMap.class);
        final StableSegmentOperationGateway<Integer, String> stableSegmentGateway = mock(
                StableSegmentOperationGateway.class);
        final SplitService<Integer, String> splitService = mock(
                SplitService.class);
        final StorageService<Integer, String> storageService = mock(
                StorageService.class);
        final ExecutorService maintenanceExecutor =
                Executors.newSingleThreadExecutor();
        final SegmentId segmentId = SegmentId.of(1);
        try {
            when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
            when(stableSegmentGateway.compact(segmentId))
                    .thenReturn(OperationResult.busy());
            when(stableSegmentGateway.flush(segmentId)).thenReturn(
                    OperationResult.busy());
            final MaintenanceService<Integer, String> maintenance = MaintenanceService
                    .<Integer, String>builder()
                    .keyToSegmentMap(keyToSegmentMap)
                    .stableSegmentGateway(stableSegmentGateway)
                    .splitService(splitService)
                    .busyBackoffMillis(1)
                    .busyTimeoutMillis(10)
                    .statsRecorder(new MaintenanceStatsRecorder())
                    .maintenanceExecutor(maintenanceExecutor)
                    .storageService(storageService)
                    .build();

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
    void builderRejectsMissingKeyToSegmentMap() {
        final MaintenanceServiceBuilder<Integer, String> builder =
                MaintenanceService.<Integer, String>builder();

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, builder::build);

        assertEquals("Property 'keyToSegmentMap' must not be null.",
                ex.getMessage());
    }

    @Test
    @SuppressWarnings("unchecked")
    void builderRejectsMissingMaintenanceExecutor() {
        final KeyToSegmentMap<Integer> keyToSegmentMap = mock(
                KeyToSegmentMap.class);
        final StableSegmentOperationGateway<Integer, String> stableSegmentGateway = mock(
                StableSegmentOperationGateway.class);
        final SplitService<Integer, String> splitService = mock(
                SplitService.class);
        final MaintenanceServiceBuilder<Integer, String> builder = MaintenanceService
                .<Integer, String>builder()
                .keyToSegmentMap(keyToSegmentMap)
                .stableSegmentGateway(stableSegmentGateway)
                .splitService(splitService)
                .busyBackoffMillis(1)
                .busyTimeoutMillis(10)
                .statsRecorder(new MaintenanceStatsRecorder());

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, builder::build);

        assertEquals("Property 'maintenanceExecutor' must not be null.",
                ex.getMessage());
    }

    @Test
    @SuppressWarnings("unchecked")
    void builderRejectsMissingStorageService() {
        final KeyToSegmentMap<Integer> keyToSegmentMap = mock(
                KeyToSegmentMap.class);
        final StableSegmentOperationGateway<Integer, String> stableSegmentGateway = mock(
                StableSegmentOperationGateway.class);
        final SplitService<Integer, String> splitService = mock(
                SplitService.class);
        final ExecutorService maintenanceExecutor =
                Executors.newSingleThreadExecutor();
        try {
            final MaintenanceServiceBuilder<Integer, String> builder =
                    MaintenanceService.<Integer, String>builder()
                            .keyToSegmentMap(keyToSegmentMap)
                            .stableSegmentGateway(stableSegmentGateway)
                            .splitService(splitService)
                            .busyBackoffMillis(1)
                            .busyTimeoutMillis(10)
                            .statsRecorder(new MaintenanceStatsRecorder())
                            .maintenanceExecutor(maintenanceExecutor);

            final IllegalArgumentException ex = assertThrows(
                    IllegalArgumentException.class, builder::build);

            assertEquals("Property 'storageService' must not be null.",
                    ex.getMessage());
        } finally {
            maintenanceExecutor.shutdownNow();
        }
    }
}
