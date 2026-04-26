package org.hestiastore.index.segmentindex.core.maintenance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.metrics.Stats;
import org.hestiastore.index.segmentindex.core.routing.IndexResult;
import org.hestiastore.index.segmentindex.core.routing.StableSegmentAccess;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class MaintenanceServiceTest {

    @Test
    void declaresMaintenanceOperations() {
        final List<String> declaredMethodNames = Arrays
                .stream(MaintenanceService.class.getDeclaredMethods())
                .filter(method -> !Modifier.isStatic(method.getModifiers()))
                .map(Method::getName)
                .sorted()
                .toList();

        assertEquals(List.of("compact", "compactAndWait", "flush",
                "flushAndWait"),
                declaredMethodNames);
    }

    @Test
    @SuppressWarnings("unchecked")
    void builderCreatesMaintenanceService() throws InterruptedException {
        final KeyToSegmentMap<Integer> keyToSegmentMap = mock(
                KeyToSegmentMap.class);
        final StableSegmentAccess<Integer, String> stableSegmentGateway = mock(
                StableSegmentAccess.class);
        final SplitService<Integer, String> splitService = mock(
                SplitService.class);
        final Runnable checkpointAction = mock(Runnable.class);
        final ExecutorService maintenanceExecutor =
                Executors.newSingleThreadExecutor();
        final SegmentId segmentId = SegmentId.of(1);
        try {
            when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(segmentId));
            when(stableSegmentGateway.compact(segmentId))
                    .thenReturn(IndexResult.busy());
            when(stableSegmentGateway.flush(segmentId)).thenReturn(
                    IndexResult.busy());
            final MaintenanceService maintenance = MaintenanceService
                    .<Integer, String>builder()
                    .logger(LoggerFactory.getLogger(
                            MaintenanceServiceTest.class))
                    .keyToSegmentMap(keyToSegmentMap)
                    .stableSegmentGateway(stableSegmentGateway)
                    .splitService(splitService)
                    .retryPolicy(new IndexRetryPolicy(1, 10))
                    .stats(new Stats())
                    .maintenanceExecutor(maintenanceExecutor)
                    .checkpointAction(checkpointAction)
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
    void builderRejectsMissingLogger() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> MaintenanceService.<Integer, String>builder().build());

        assertEquals("Property 'logger' must not be null.", ex.getMessage());
    }

    @Test
    @SuppressWarnings("unchecked")
    void builderRejectsMissingMaintenanceExecutor() {
        final KeyToSegmentMap<Integer> keyToSegmentMap = mock(
                KeyToSegmentMap.class);
        final StableSegmentAccess<Integer, String> stableSegmentGateway = mock(
                StableSegmentAccess.class);
        final SplitService<Integer, String> splitService = mock(
                SplitService.class);

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> MaintenanceService.<Integer, String>builder()
                        .logger(LoggerFactory.getLogger(
                                MaintenanceServiceTest.class))
                        .keyToSegmentMap(keyToSegmentMap)
                        .stableSegmentGateway(stableSegmentGateway)
                        .splitService(splitService)
                        .retryPolicy(new IndexRetryPolicy(1, 10))
                        .stats(new Stats())
                        .build());

        assertEquals("Property 'maintenanceExecutor' must not be null.",
                ex.getMessage());
    }

    @Test
    @SuppressWarnings("unchecked")
    void builderRejectsMissingCheckpointAction() {
        final KeyToSegmentMap<Integer> keyToSegmentMap = mock(
                KeyToSegmentMap.class);
        final StableSegmentAccess<Integer, String> stableSegmentGateway = mock(
                StableSegmentAccess.class);
        final SplitService<Integer, String> splitService = mock(
                SplitService.class);
        final ExecutorService maintenanceExecutor =
                Executors.newSingleThreadExecutor();
        try {
            final IllegalArgumentException ex = assertThrows(
                    IllegalArgumentException.class,
                    () -> MaintenanceService.<Integer, String>builder()
                            .logger(LoggerFactory.getLogger(
                                    MaintenanceServiceTest.class))
                            .keyToSegmentMap(keyToSegmentMap)
                            .stableSegmentGateway(stableSegmentGateway)
                            .splitService(splitService)
                            .retryPolicy(new IndexRetryPolicy(1, 10))
                            .stats(new Stats())
                            .maintenanceExecutor(maintenanceExecutor)
                            .build());

            assertEquals("Property 'checkpointAction' must not be null.",
                    ex.getMessage());
        } finally {
            maintenanceExecutor.shutdownNow();
        }
    }
}
