package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.hestiastore.index.segmentregistry.SegmentFactory;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentRuntimeLimitApplierTest {

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private SegmentFactory<Integer, String> segmentFactory;

    @Mock
    private Segment<Integer, String> firstSegment;

    @Mock
    private Segment<Integer, String> secondSegment;

    private SegmentRuntimeLimitApplier<Integer, String> applier;

    @BeforeEach
    void setUp() {
        applier = new SegmentRuntimeLimitApplier<>(segmentRegistry,
                segmentFactory);
    }

    @Test
    void applyUpdatesRegistryFactoryAndLoadedSegments() {
        when(segmentRegistry.loadedSegmentsSnapshot())
                .thenReturn(List.of(firstSegment, secondSegment));
        final Map<RuntimeSettingKey, Integer> effective = Map.of(
                RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                Integer.valueOf(3),
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE,
                Integer.valueOf(10),
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION,
                Integer.valueOf(5),
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER,
                Integer.valueOf(7));

        applier.apply(effective);

        verify(segmentRegistry).updateCacheLimit(3);
        verify(segmentFactory).updateRuntimeLimits(10, 5, 7);
        final ArgumentCaptor<SegmentRuntimeLimits> captor = ArgumentCaptor
                .forClass(SegmentRuntimeLimits.class);
        verify(firstSegment).applyRuntimeLimits(captor.capture());
        verify(secondSegment).applyRuntimeLimits(captor.getValue());
        assertEquals(new SegmentRuntimeLimits(10, 5, 7),
                captor.getValue());
    }
}
