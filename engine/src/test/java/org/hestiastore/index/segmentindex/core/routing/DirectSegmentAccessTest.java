package org.hestiastore.index.segmentindex.core.routing;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.segmentaccess.SegmentAccessService;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;

class DirectSegmentAccessTest {

    @Test
    void createReturnsDirectSegmentAccess() {
        final DirectSegmentAccess<Integer, String> access =
                DirectSegmentAccess.create(mockKeyToSegmentMap(),
                        mockSegmentRegistry(), mockSegmentAccessService(),
                        mock(IndexRetryPolicy.class));

        assertNotNull(access);
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
    private SegmentAccessService<Integer, String> mockSegmentAccessService() {
        return mock(SegmentAccessService.class);
    }
}
