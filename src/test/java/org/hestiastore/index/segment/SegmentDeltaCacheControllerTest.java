package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentDeltaCacheControllerTest {

    @Mock
    private SegmentFiles<Integer, Integer> segmentFiles;

    @Mock
    private SegmentPropertiesManager segmentPropertiesManager;

    @Mock
    private SegmentResources<Integer, Integer> segmentResources;

    @Mock
    private SegmentDeltaCache<Integer, Integer> deltaCache;

    @Test
    void clear_evicted_segment_cache() {
        final SegmentDeltaCacheController<Integer, Integer> controller = new SegmentDeltaCacheController<>(
                segmentFiles, segmentPropertiesManager, segmentResources, 10, 5,
                2);
        final SegmentCache<Integer, Integer> segmentCache = new SegmentCache<>(
                new TypeDescriptorInteger().getComparator(),
                new TypeDescriptorInteger());
        segmentCache.putToWriteCache(Entry.of(1, 11));
        segmentCache.putToWriteCache(Entry.of(2, 22));
        controller.setSegmentCache(segmentCache);

        controller.clear();

        assertEquals(0, segmentCache.size());
    }

    @Test
    void sizesPreferSegmentCacheWhenAvailable() {
        when(segmentResources.getSegmentDeltaCache()).thenReturn(deltaCache);
        when(deltaCache.size()).thenReturn(10);
        when(deltaCache.sizeWithoutTombstones()).thenReturn(7);
        final SegmentDeltaCacheController<Integer, Integer> controller = new SegmentDeltaCacheController<>(
                segmentFiles, segmentPropertiesManager, segmentResources, 10, 5,
                2);

        assertEquals(10, controller.getDeltaCacheSize());
        assertEquals(7, controller.getDeltaCacheSizeWithoutTombstones());

        final TypeDescriptorInteger typeDescriptor = new TypeDescriptorInteger();
        final SegmentCache<Integer, Integer> segmentCache = new SegmentCache<>(
                typeDescriptor.getComparator(), typeDescriptor,
                java.util.List.of(Entry.of(1, 11)));
        segmentCache
                .putToWriteCache(Entry.of(2, typeDescriptor.getTombstone()));
        segmentCache.putToWriteCache(Entry.of(3, 33));
        controller.setSegmentCache(segmentCache);

        assertEquals(3, controller.getDeltaCacheSize());
        assertEquals(2, controller.getDeltaCacheSizeWithoutTombstones());
    }
}
