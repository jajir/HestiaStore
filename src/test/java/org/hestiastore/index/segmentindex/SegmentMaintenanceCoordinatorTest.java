package org.hestiastore.index.segmentindex;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentStats;
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

    @Test
    void returnsEarlyWhenWriteCacheLimitMissing() {
        when(conf.getMaxNumberOfKeysInSegmentWriteCache()).thenReturn(null);

        final SegmentMaintenanceCoordinator<String, String> coordinator = new SegmentMaintenanceCoordinator<>(
                conf, keySegmentCache, segmentRegistry);

        coordinator.handlePostWrite(segment, "key", SegmentId.of(1), 1L);

        verifyNoInteractions(segment, keySegmentCache, segmentRegistry);
    }

    @Test
    void checksCacheSizeWhenEligible() {
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentStats stats = mock(SegmentStats.class);
        when(conf.getMaxNumberOfKeysInSegmentWriteCache()).thenReturn(1);
        when(segment.getNumberOfKeysInWriteCache()).thenReturn(0);
        when(segment.wasClosed()).thenReturn(false);
        when(segmentRegistry.isSegmentInstance(segmentId, segment))
                .thenReturn(true);
        when(keySegmentCache.isKeyMappedToSegment("key", segmentId))
                .thenReturn(true);
        when(keySegmentCache.isMappingValid("key", segmentId, 7L))
                .thenReturn(true);
        when(conf.getMaxNumberOfKeysInSegmentCache()).thenReturn(10);
        when(segment.getStats()).thenReturn(stats);
        when(stats.getNumberOfKeysInDeltaCache()).thenReturn(0L);
        when(segment.getNumberOfKeysInCache()).thenReturn(10L);

        final SegmentMaintenanceCoordinator<String, String> coordinator = new SegmentMaintenanceCoordinator<>(
                conf, keySegmentCache, segmentRegistry);

        coordinator.handlePostWrite(segment, "key", segmentId, 7L);

        verify(segment).getNumberOfKeysInCache();
    }
}
