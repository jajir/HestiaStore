package org.hestiastore.index.segmentindex.core.segmentaccess;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;

class SegmentAccessServiceBuilderTest {

    @Test
    void buildCreatesServiceFromDependencies() {
        final SegmentAccessService<Integer, String> service =
                SegmentAccessService.<Integer, String>builder()
                        .keyToSegmentMap(mockKeyToSegmentMap())
                        .segmentRegistry(mockSegmentRegistry())
                        .segmentTopology(mockSegmentTopology())
                        .retryPolicy(mock(IndexRetryPolicy.class))
                        .build();

        assertNotNull(service);
    }

    @Test
    void buildRejectsMissingKeyToSegmentMap() {
        final SegmentAccessServiceBuilder<Integer, String> builder =
                SegmentAccessService.<Integer, String>builder()
                        .segmentRegistry(mockSegmentRegistry())
                        .segmentTopology(mockSegmentTopology())
                        .retryPolicy(mock(IndexRetryPolicy.class));

        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void keyToSegmentMapRejectsNull() {
        final SegmentAccessServiceBuilder<Integer, String> builder =
                SegmentAccessService.builder();

        assertThrows(IllegalArgumentException.class,
                () -> builder.keyToSegmentMap(null));
    }

    @SuppressWarnings("unchecked")
    private KeyToSegmentMap<Integer> mockKeyToSegmentMap() {
        return mock(KeyToSegmentMap.class);
    }

    @SuppressWarnings("unchecked")
    private SegmentRegistry<Integer, String> mockSegmentRegistry() {
        return mock(SegmentRegistry.class);
    }

    @SuppressWarnings("unchecked")
    private SegmentTopology<Integer> mockSegmentTopology() {
        return mock(SegmentTopology.class);
    }
}
