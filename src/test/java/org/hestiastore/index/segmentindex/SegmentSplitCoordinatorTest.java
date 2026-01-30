package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitCoordinatorTest {

    @Mock
    private Segment<Integer, String> segment;

    @Mock
    private KeyToSegmentMap<Integer> keyToSegmentMap;

    private KeyToSegmentMapSynchronizedAdapter<Integer> synchronizedKeyToSegmentMap;

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private SegmentRegistryAccess<Integer, String> registryAccess;

    private SegmentWriterTxFactory<Integer, String> writerTxFactory;

    private SegmentSplitCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
        writerTxFactory = id -> {
            throw new IllegalStateException("writerTxFactory not configured");
        };
        coordinator = new SegmentSplitCoordinator<>(
                IndexConfiguration.<Integer, String>builder().build(),
                synchronizedKeyToSegmentMap, segmentRegistry, registryAccess,
                writerTxFactory);
    }

    @AfterEach
    void tearDown() {
        coordinator = null;
        synchronizedKeyToSegmentMap = null;
        writerTxFactory = null;
    }

    @Test
    void shouldBeSplit_reflectsCacheThreshold() {
        when(segment.getNumberOfKeysInCache()).thenReturn(9L, 10L);

        assertFalse(coordinator.shouldBeSplit(segment, 10));
        assertTrue(coordinator.shouldBeSplit(segment, 10));
    }

    @Test
    void optionallySplit_shortCircuitsWhenBelowThreshold() {
        when(segment.getNumberOfKeysInCache()).thenReturn(1L);

        assertFalse(coordinator.optionallySplit(segment, 100L));
        verifyNoInteractions(segmentRegistry);
    }

    @Test
    void optionallySplit_doesNotInvokeCompaction() {
        when(segment.getNumberOfKeysInCache()).thenReturn(10L);
        final SegmentId segmentId = SegmentId.of(1);
        when(segment.getId()).thenReturn(segmentId);
        when(registryAccess.isSegmentInstance(segmentId, segment))
                .thenReturn(false);

        coordinator.optionallySplit(segment, 5L);

        verify(segment, never()).compact();
    }
}
