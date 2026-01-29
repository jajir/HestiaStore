package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segmentregistry.SegmentRegistryMaintenance;
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
    private SegmentRegistryMaintenance<Integer, String> segmentRegistry;

    private SegmentSplitCoordinator<Integer, String> coordinator;

    @BeforeEach
    void setUp() {
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
        coordinator = new SegmentSplitCoordinator<>(
                IndexConfiguration.<Integer, String>builder().build(),
                synchronizedKeyToSegmentMap, segmentRegistry);
    }

    @AfterEach
    void tearDown() {
        coordinator = null;
        synchronizedKeyToSegmentMap = null;
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
        final EntryIterator<Integer, String> iterator = new EntryIteratorList<>(
                List.of(Entry.of(1, "a")));
        when(segment.openIterator(SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(SegmentResult.ok(iterator));

        coordinator.optionallySplit(segment, 5L);

        verify(segment, never()).compact();
    }
}
