package org.hestiastore.index.segmentindex.configuration.tuning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;

import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.hestiastore.index.segmentregistry.BlockingSegment;
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
    private BlockingSegment<Integer, String> firstSegment;

    @Mock
    private BlockingSegment<Integer, String> secondSegment;

    @Mock
    private BlockingSegment.Runtime firstRuntime;

    @Mock
    private BlockingSegment.Runtime secondRuntime;

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
        final RuntimeTuningSnapshot effective = new RuntimeTuningSnapshot(
                "segment-runtime-limit-applier-test", 0L, Instant.now(),
                new RuntimeSegmentTuningSnapshot(10, 3),
                new RuntimeWritePathTuningSnapshot(5, 7, 9, 50));

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
