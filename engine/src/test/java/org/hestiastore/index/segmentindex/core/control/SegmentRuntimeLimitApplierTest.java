package org.hestiastore.index.segmentindex.core.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.hestiastore.index.control.model.RuntimeSettingKey;
import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.hestiastore.index.segmentregistry.SegmentHandle;
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
    private SegmentRegistry.Runtime<Integer, String> segmentRuntime;

    @Mock
    private SegmentHandle<Integer, String> firstSegment;

    @Mock
    private SegmentHandle<Integer, String> secondSegment;

    @Mock
    private SegmentHandle.Runtime firstRuntime;

    @Mock
    private SegmentHandle.Runtime secondRuntime;

    private SegmentRuntimeLimitApplier<Integer, String> applier;

    @BeforeEach
    void setUp() {
        applier = new SegmentRuntimeLimitApplier<>(segmentRegistry,
                segmentRuntime);
    }

    @Test
    void applyUpdatesRegistryFactoryAndLoadedSegments() {
        when(firstSegment.getRuntime()).thenReturn(firstRuntime);
        when(secondSegment.getRuntime()).thenReturn(secondRuntime);
        when(segmentRuntime.loadedSegmentsSnapshot())
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

        final SegmentRuntimeLimits expectedLimits = new SegmentRuntimeLimits(10,
                5, 7);
        verify(segmentRegistry).updateCacheLimit(3);
        final ArgumentCaptor<SegmentRuntimeLimits> tunerCaptor = ArgumentCaptor
                .forClass(SegmentRuntimeLimits.class);
        verify(segmentRuntime).updateRuntimeLimits(tunerCaptor.capture());
        assertEquals(expectedLimits, tunerCaptor.getValue());
        final ArgumentCaptor<SegmentRuntimeLimits> segmentCaptor = ArgumentCaptor
                .forClass(SegmentRuntimeLimits.class);
        verify(firstRuntime).updateRuntimeLimits(segmentCaptor.capture());
        verify(secondRuntime).updateRuntimeLimits(segmentCaptor.capture());
        assertEquals(List.of(expectedLimits, expectedLimits),
                segmentCaptor.getAllValues());
    }
}
