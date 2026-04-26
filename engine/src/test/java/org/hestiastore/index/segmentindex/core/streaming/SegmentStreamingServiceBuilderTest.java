package org.hestiastore.index.segmentindex.core.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationResult;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationAccess;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class SegmentStreamingServiceBuilderTest {

    @Test
    @SuppressWarnings("unchecked")
    void buildCreatesSegmentStreamingService() {
        final KeyToSegmentMap<Integer> keyToSegmentMap = mock(
                KeyToSegmentMap.class);
        final SegmentRegistry<Integer, String> segmentRegistry = mock(
                SegmentRegistry.class);
        final StableSegmentOperationAccess<Integer, String> stableSegmentGateway = mock(
                StableSegmentOperationAccess.class);
        final SegmentId segmentId = SegmentId.of(1);
        final EntryIterator<Integer, String> iterator = EntryIterator
                .make(List.<Entry<Integer, String>>of().iterator());
        when(stableSegmentGateway.openIterator(segmentId,
                SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(StableSegmentOperationResult.ok(iterator));

        final SegmentStreamingService<Integer, String> service =
                SegmentStreamingService.<Integer, String>builder()
                        .logger(LoggerFactory.getLogger(
                                SegmentStreamingServiceBuilderTest.class))
                        .keyToSegmentMap(keyToSegmentMap)
                        .segmentRegistry(segmentRegistry)
                        .stableSegmentGateway(stableSegmentGateway)
                        .retryPolicy(new IndexRetryPolicy(1, 10))
                        .build();

        assertSame(iterator, service.openIterator(segmentId,
                SegmentIteratorIsolation.FAIL_FAST));
        verify(stableSegmentGateway).openIterator(segmentId,
                SegmentIteratorIsolation.FAIL_FAST);
    }

    @Test
    void buildRejectsMissingLogger() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> SegmentStreamingService.<Integer, String>builder()
                        .build());

        assertEquals("Property 'logger' must not be null.", ex.getMessage());
    }
}
