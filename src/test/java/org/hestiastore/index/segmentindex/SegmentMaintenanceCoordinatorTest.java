package org.hestiastore.index.segmentindex;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentStats;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentMaintenanceCoordinatorTest {

    @Mock
    private IndexConfiguration<String, String> conf;

    @Mock
    private KeySegmentCache<String> keySegmentCache;

    @Mock
    private SegmentRegistry<String, String> segmentRegistry;

    @Mock
    private Segment<String, String> segment;

    @Mock
    private ExecutorService maintenanceExecutor;

    @BeforeEach
    void setUp() {
        when(segmentRegistry.getSplitExecutor()).thenReturn(maintenanceExecutor);
        when(conf.getIndexBusyBackoffMillis()).thenReturn(1);
        when(conf.getIndexBusyTimeoutMillis()).thenReturn(1000);
    }

    @Test
    void returnsEarlyWhenWriteCacheLimitMissing() {
        when(conf.getMaxNumberOfKeysInSegmentWriteCache()).thenReturn(null);

        final SegmentMaintenanceCoordinator<String, String> coordinator = new SegmentMaintenanceCoordinator<>(
                conf, keySegmentCache, segmentRegistry);

        coordinator.handlePostWrite(segment, "key", SegmentId.of(1), 1L);

        verifyNoInteractions(segment, keySegmentCache);
        verify(segmentRegistry).getSplitExecutor();
        verifyNoMoreInteractions(segmentRegistry);
    }

    @Test
    void checksCacheSizeWhenEligible() {
        final SegmentId segmentId = SegmentId.of(1);
        when(conf.getMaxNumberOfKeysInSegmentWriteCache()).thenReturn(1);
        when(segment.wasClosed()).thenReturn(false);
        when(segmentRegistry.isSegmentInstance(segmentId, segment))
                .thenReturn(true);
        when(keySegmentCache.isKeyMappedToSegment("key", segmentId))
                .thenReturn(true);
        when(keySegmentCache.isMappingValid("key", segmentId, 7L))
                .thenReturn(true);
        when(conf.getMaxNumberOfKeysInSegmentCache()).thenReturn(10);
        when(segment.getNumberOfKeysInCache()).thenReturn(10L);

        final SegmentMaintenanceCoordinator<String, String> coordinator = new SegmentMaintenanceCoordinator<>(
                conf, keySegmentCache, segmentRegistry);

        coordinator.handlePostWrite(segment, "key", segmentId, 7L);

        verify(segment).getNumberOfKeysInCache();
    }

    @Test
    void flushIsScheduledWhenWriteCacheThresholdReached() {
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentStats stats = mock(SegmentStats.class);
        when(conf.getMaxNumberOfKeysInSegmentWriteCache()).thenReturn(2);
        when(conf.getMaxNumberOfKeysInSegmentCache()).thenReturn(10);
        when(conf.isSegmentMaintenanceAutoEnabled()).thenReturn(true);
        prepareEligibleSegment(segmentId);
        when(segment.getNumberOfKeysInWriteCache()).thenReturn(2);
        when(segment.getStats()).thenReturn(stats);
        when(stats.getNumberOfKeysInDeltaCache()).thenReturn(0L);
        when(segment.getNumberOfKeysInCache()).thenReturn(0L);
        when(segment.flush()).thenReturn(okMaintenance());

        final SegmentMaintenanceCoordinator<String, String> coordinator = new SegmentMaintenanceCoordinator<>(
                conf, keySegmentCache, segmentRegistry);

        coordinator.handlePostWrite(segment, "key", segmentId, 7L);

        verify(segment).flush();
        verify(segment, never()).compact();
    }

    @Test
    void compactIsScheduledWhenDeltaCacheThresholdReached() {
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentStats stats = mock(SegmentStats.class);
        when(conf.getMaxNumberOfKeysInSegmentWriteCache()).thenReturn(10);
        when(conf.getMaxNumberOfKeysInSegmentCache()).thenReturn(1);
        when(conf.isSegmentMaintenanceAutoEnabled()).thenReturn(true);
        prepareEligibleSegment(segmentId);
        when(segment.getNumberOfKeysInWriteCache()).thenReturn(0);
        when(segment.getStats()).thenReturn(stats);
        when(stats.getNumberOfKeysInDeltaCache()).thenReturn(1L);
        when(segment.getNumberOfKeysInCache()).thenReturn(0L);
        when(segment.compact()).thenReturn(okMaintenance());

        final SegmentMaintenanceCoordinator<String, String> coordinator = new SegmentMaintenanceCoordinator<>(
                conf, keySegmentCache, segmentRegistry);

        coordinator.handlePostWrite(segment, "key", segmentId, 7L);

        verify(segment).compact();
        verify(segment, never()).flush();
    }

    @Test
    void flushAndCompactAreScheduledWhenBothThresholdsReached() {
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentStats stats = mock(SegmentStats.class);
        when(conf.getMaxNumberOfKeysInSegmentWriteCache()).thenReturn(1);
        when(conf.getMaxNumberOfKeysInSegmentCache()).thenReturn(1);
        when(conf.isSegmentMaintenanceAutoEnabled()).thenReturn(true);
        prepareEligibleSegment(segmentId);
        when(segment.getNumberOfKeysInWriteCache()).thenReturn(1);
        when(segment.getStats()).thenReturn(stats);
        when(stats.getNumberOfKeysInDeltaCache()).thenReturn(1L);
        when(segment.getNumberOfKeysInCache()).thenReturn(0L);
        when(segment.flush()).thenReturn(okMaintenance());
        when(segment.compact()).thenReturn(okMaintenance());

        final SegmentMaintenanceCoordinator<String, String> coordinator = new SegmentMaintenanceCoordinator<>(
                conf, keySegmentCache, segmentRegistry);

        coordinator.handlePostWrite(segment, "key", segmentId, 7L);

        verify(segment).flush();
        verify(segment).compact();
    }

    @Test
    void autoMaintenanceCanBeDisabled() {
        final SegmentId segmentId = SegmentId.of(1);
        when(conf.getMaxNumberOfKeysInSegmentWriteCache()).thenReturn(1);
        when(conf.getMaxNumberOfKeysInSegmentCache()).thenReturn(1);
        when(conf.isSegmentMaintenanceAutoEnabled()).thenReturn(false);
        prepareEligibleSegment(segmentId);
        when(segment.getNumberOfKeysInCache()).thenReturn(0L);

        final SegmentMaintenanceCoordinator<String, String> coordinator = new SegmentMaintenanceCoordinator<>(
                conf, keySegmentCache, segmentRegistry);

        coordinator.handlePostWrite(segment, "key", segmentId, 7L);

        verify(segment, never()).flush();
        verify(segment, never()).compact();
    }

    private void prepareEligibleSegment(final SegmentId segmentId) {
        when(segment.wasClosed()).thenReturn(false);
        when(segmentRegistry.isSegmentInstance(segmentId, segment))
                .thenReturn(true);
        when(keySegmentCache.isKeyMappedToSegment("key", segmentId))
                .thenReturn(true);
        when(keySegmentCache.isMappingValid("key", segmentId, 7L))
                .thenReturn(true);
    }

    private SegmentResult<CompletionStage<Void>> okMaintenance() {
        return SegmentResult.ok(CompletableFuture.completedFuture(null));
    }
}
