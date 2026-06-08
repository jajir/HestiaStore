package org.hestiastore.index.segmentindex.core.segmentlease;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.hestiastore.index.segmentindex.core.topology.SegmentTopology;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;

class SegmentLeaseServiceBuilderTest {

    @Test
    void buildCreatesServiceFromDependencies() {
        final SegmentLeaseService<Integer, String> service =
                SegmentLeaseService.<Integer, String>builder()
                        .keyToSegmentMap(mockKeyToSegmentMap())
                        .segmentRegistry(mockSegmentRegistry())
                        .segmentTopology(mockSegmentTopology())
                        .busyBackoffMillis(1)
                        .busyTimeoutMillis(10)
                        .build();

        assertNotNull(service);
    }

    @Test
    void buildRejectsMissingKeyToSegmentMap() {
        final SegmentLeaseServiceBuilder<Integer, String> builder =
                SegmentLeaseService.<Integer, String>builder()
                        .segmentRegistry(mockSegmentRegistry())
                        .segmentTopology(mockSegmentTopology())
                        .busyBackoffMillis(1)
                        .busyTimeoutMillis(10);

        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void keyToSegmentMapRejectsNull() {
        final SegmentLeaseServiceBuilder<Integer, String> builder =
                SegmentLeaseService.builder();

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
