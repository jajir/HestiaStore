package org.hestiastore.index.segmentindex.core.streaming;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationResult;
import org.hestiastore.index.segmentindex.core.stablesegment.StableSegmentOperationAccess;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapImpl;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
class SegmentStreamingServiceImplTest {

    @Mock
    private SegmentRegistry<String, String> segmentRegistry;

    @Mock
    private StableSegmentOperationAccess<String, String> stableSegmentGateway;

    @Mock
    private BlockingSegment<String, String> segmentHandle;

    private Directory directory;
    private KeyToSegmentMapImpl<String> keyToSegmentMap;
    private KeyToSegmentMap<String> synchronizedKeyToSegmentMap;
    private SegmentStreamingServiceImpl<String, String> service;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        keyToSegmentMap = new KeyToSegmentMapImpl<>(directory,
                new TypeDescriptorShortString());
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
        service = new SegmentStreamingServiceImpl<>(
                LoggerFactory.getLogger(SegmentStreamingServiceImplTest.class),
                synchronizedKeyToSegmentMap, segmentRegistry,
                stableSegmentGateway, new IndexRetryPolicy(1, 10));
    }

    @AfterEach
    void tearDown() {
        if (synchronizedKeyToSegmentMap != null
                && !synchronizedKeyToSegmentMap.wasClosed()) {
            synchronizedKeyToSegmentMap.close();
        }
    }

    @Test
    void invalidateIterators_invalidatesLoadedMappedSegments() {
        final SegmentId segmentId = createBootstrapSegment("key");
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenReturn(Optional.of(segmentHandle));

        service.invalidateIterators();

        verify(segmentHandle).invalidateIterators();
    }

    @Test
    void invalidateIterators_ignoresLookupFailureForMappedSegment() {
        final SegmentId segmentId = createBootstrapSegment("key");
        when(segmentRegistry.tryGetSegment(segmentId))
                .thenThrow(new IndexException("boom"));

        assertDoesNotThrow(() -> service.invalidateIterators());
    }

    @Test
    void openIterator_returnsIteratorFromCore() {
        final SegmentId segmentId = createBootstrapSegment("key");
        final EntryIterator<String, String> iterator = EntryIterator
                .make(List.<Entry<String, String>>of().iterator());
        when(stableSegmentGateway.openIterator(segmentId,
                SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(StableSegmentOperationResult.ok(iterator));

        final EntryIterator<String, String> result = service.openIterator(
                segmentId, SegmentIteratorIsolation.FAIL_FAST);

        assertSame(iterator, result);
        verify(stableSegmentGateway).openIterator(segmentId,
                SegmentIteratorIsolation.FAIL_FAST);
    }

    @Test
    void openIterator_retriesBusyAndFailsForError() {
        final SegmentId busySegmentId = createBootstrapSegment("busy-key");
        final EntryIterator<String, String> iterator = EntryIterator
                .make(List.<Entry<String, String>>of().iterator());
        when(stableSegmentGateway.openIterator(busySegmentId,
                SegmentIteratorIsolation.FAIL_FAST))
                        .thenReturn(StableSegmentOperationResult.busy(),
                                StableSegmentOperationResult.ok(iterator));

        assertSame(iterator, service.openIterator(busySegmentId,
                SegmentIteratorIsolation.FAIL_FAST));

        final SegmentId errorSegmentId = createBootstrapSegment("error-key");
        when(stableSegmentGateway.openIterator(errorSegmentId,
                SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(StableSegmentOperationResult.error());

        assertThrows(IndexException.class,
                () -> service.openIterator(errorSegmentId,
                        SegmentIteratorIsolation.FAIL_FAST));
    }

    private SegmentId createBootstrapSegment(final String key) {
        synchronizedKeyToSegmentMap.extendMaxKeyIfNeeded(key);
        return synchronizedKeyToSegmentMap.findSegmentIdForKey(key);
    }
}
