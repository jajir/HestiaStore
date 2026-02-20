package org.hestiastore.index.segmentindex;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
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
    private KeyToSegmentMap<String> keyToSegmentMap;

    private KeyToSegmentMapSynchronizedAdapter<String> synchronizedKeyToSegmentMap;

    @Mock
    private Segment<String, String> segment;

    @Mock
    private SegmentAsyncSplitCoordinator<String, String> splitCoordinator;

    @BeforeEach
    void setUp() {
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
    }

    @Test
    void returnsEarlyWhenWriteCacheLimitMissing() {
        when(conf.getMaxNumberOfKeysInSegmentWriteCache()).thenReturn(null);

        final SegmentMaintenanceCoordinator<String, String> coordinator = new SegmentMaintenanceCoordinator<>(
                conf, synchronizedKeyToSegmentMap, splitCoordinator);

        coordinator.handlePostWrite(segment, "key", SegmentId.of(1), 1L);

        verifyNoInteractions(segment, keyToSegmentMap, splitCoordinator);
    }

    @Test
    void checksCacheSizeWhenEligible() {
        final SegmentId segmentId = SegmentId.of(1);
        when(conf.getMaxNumberOfKeysInSegmentWriteCache()).thenReturn(1);
        when(segment.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.isKeyMappedToSegment("key", segmentId))
                .thenReturn(true);
        when(keyToSegmentMap.isMappingValid("key", segmentId, 7L))
                .thenReturn(true);
        when(conf.getMaxNumberOfKeysInSegment()).thenReturn(100);
        when(segment.getNumberOfKeysInCache()).thenReturn(10L);

        final SegmentMaintenanceCoordinator<String, String> coordinator = new SegmentMaintenanceCoordinator<>(
                conf, synchronizedKeyToSegmentMap, splitCoordinator);

        coordinator.handlePostWrite(segment, "key", segmentId, 7L);

        verify(segment).getNumberOfKeysInCache();
    }

    @Test
    void usesSegmentSizeThreshold_notSegmentCacheThreshold() {
        final SegmentId segmentId = SegmentId.of(1);
        when(conf.getMaxNumberOfKeysInSegmentWriteCache()).thenReturn(1);
        when(segment.getState()).thenReturn(SegmentState.READY);
        when(keyToSegmentMap.isKeyMappedToSegment("key", segmentId))
                .thenReturn(true);
        when(keyToSegmentMap.isMappingValid("key", segmentId, 7L))
                .thenReturn(true);
        when(conf.getMaxNumberOfKeysInSegment()).thenReturn(1000);
        when(segment.getNumberOfKeysInCache()).thenReturn(50L);

        final SegmentMaintenanceCoordinator<String, String> coordinator = new SegmentMaintenanceCoordinator<>(
                conf, synchronizedKeyToSegmentMap, splitCoordinator);

        coordinator.handlePostWrite(segment, "key", segmentId, 7L);

        verify(conf).getMaxNumberOfKeysInSegment();
        verify(conf, never()).getMaxNumberOfKeysInSegmentCache();
        verifyNoInteractions(splitCoordinator);
    }

}
