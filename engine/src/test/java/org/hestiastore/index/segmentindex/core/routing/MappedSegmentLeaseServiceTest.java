package org.hestiastore.index.segmentindex.core.routing;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.hestiastore.index.segmentindex.routemap.SegmentRouteMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;

class MappedSegmentLeaseServiceTest {

    @Test
    void createBuildsServiceFromDependencies() {
        final MappedSegmentLeaseService<Integer, String> service = MappedSegmentLeaseService.create(mockKeyToSegmentMap(),
                mockSegmentRegistry(), mockSegmentTopology(), 1, 10);

        assertNotNull(service);
    }

    @Test
    void createRejectsMissingKeyToSegmentMap() {
        assertThrows(IllegalArgumentException.class,
                () -> MappedSegmentLeaseService.create(null,
                        mockSegmentRegistry(), mockSegmentTopology(), 1, 10));
    }

    @SuppressWarnings("unchecked")
    private SegmentRouteMap<Integer> mockKeyToSegmentMap() {
        return mock(SegmentRouteMap.class);
    }

    @SuppressWarnings("unchecked")
    private SegmentRegistry<Integer, String> mockSegmentRegistry() {
        return mock(SegmentRegistry.class);
    }

    @SuppressWarnings("unchecked")
    private RouteTopology<Integer> mockSegmentTopology() {
        return mock(RouteTopology.class);
    }
}
