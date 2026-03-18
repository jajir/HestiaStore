package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMapSynchronizedAdapter;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
class StableSegmentCoordinatorTest {

    @Mock
    private SegmentRegistry<String, String> segmentRegistry;

    @Mock
    private BackgroundSplitCoordinator<String, String> backgroundSplitCoordinator;

    @Mock
    private SegmentIndexCore<String, String> core;

    @Mock
    private Segment<String, String> segment;

    private Directory directory;
    private KeyToSegmentMap<String> keyToSegmentMap;
    private KeyToSegmentMapSynchronizedAdapter<String> synchronizedKeyToSegmentMap;
    private StableSegmentCoordinator<String, String> coordinator;

    @BeforeEach
    void setUp() {
        directory = new MemDirectory();
        keyToSegmentMap = new KeyToSegmentMap<>(directory,
                new TypeDescriptorShortString());
        synchronizedKeyToSegmentMap = new KeyToSegmentMapSynchronizedAdapter<>(
                keyToSegmentMap);
        coordinator = new StableSegmentCoordinator<>(
                LoggerFactory.getLogger(StableSegmentCoordinatorTest.class),
                synchronizedKeyToSegmentMap, segmentRegistry,
                backgroundSplitCoordinator, core, new IndexRetryPolicy(1, 10),
                new Stats());
    }

    @AfterEach
    void tearDown() {
        if (synchronizedKeyToSegmentMap != null
                && !synchronizedKeyToSegmentMap.wasClosed()) {
            synchronizedKeyToSegmentMap.close();
        }
    }

    @Test
    void putEntryForDrain_writesToLoadedSegment() {
        final SegmentId segmentId = keyToSegmentMap.insertKeyToSegment("key");
        when(segmentRegistry.getSegment(segmentId))
                .thenReturn(SegmentRegistryResult.ok(segment));
        when(segment.put("key", "value")).thenReturn(SegmentResult.ok());

        assertDoesNotThrow(
                () -> coordinator.putEntryForDrain(segmentId, "key", "value"));
        verify(segment).put("key", "value");
    }

    @Test
    void invalidateIterators_invalidatesLoadedMappedSegments() {
        final SegmentId segmentId = keyToSegmentMap.insertKeyToSegment("key");
        when(segmentRegistry.getSegment(segmentId))
                .thenReturn(SegmentRegistryResult.ok(segment));

        coordinator.invalidateIterators();

        verify(segment).invalidateIterators();
    }

    @Test
    void openIteratorWithRetry_returnsIteratorFromCore() {
        final SegmentId segmentId = keyToSegmentMap.insertKeyToSegment("key");
        final EntryIterator<String, String> iterator = EntryIterator
                .make(List.<Entry<String, String>>of().iterator());
        when(core.openIterator(segmentId, SegmentIteratorIsolation.FAIL_FAST))
                .thenReturn(IndexResult.ok(iterator));

        coordinator.openIteratorWithRetry(segmentId,
                SegmentIteratorIsolation.FAIL_FAST);

        verify(core).openIterator(segmentId,
                SegmentIteratorIsolation.FAIL_FAST);
    }
}
