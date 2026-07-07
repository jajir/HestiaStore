package org.hestiastore.index.segmentregistry;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;

/**
 * Test-only access to private registry cache entries for cross-package tests.
 */
public final class SegmentRegistryCacheTestSupport {

    private SegmentRegistryCacheTestSupport() {
    }

    /**
     * Inserts a ready entry into a registry cache object.
     *
     * @param cache     registry cache instance
     * @param segmentId segment id
     * @param segment   ready segment
     */
    public static <K, V> void putReadyEntry(final Object cache,
            final SegmentId segmentId, final Segment<K, V> segment) {
        try {
            final SegmentRegistryEntry<K, V> entry = new SegmentRegistryEntry<>(
                    0L);
            entry.finishLoad(segment);
            if (readMap(cache).put(segmentId, entry) == null) {
                readSize(cache).incrementAndGet();
            }
        } catch (final ReflectiveOperationException ex) {
            throw new IllegalStateException(
                    "Unable to update registry cache for test", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentHashMap<SegmentId, Object> readMap(
            final Object cache) throws ReflectiveOperationException {
        final Field mapField = cache.getClass().getDeclaredField("map");
        mapField.setAccessible(true);
        return (ConcurrentHashMap<SegmentId, Object>) mapField.get(cache);
    }

    private static AtomicInteger readSize(final Object cache)
            throws ReflectiveOperationException {
        final Field sizeField = cache.getClass().getDeclaredField("size");
        sizeField.setAccessible(true);
        return (AtomicInteger) sizeField.get(cache);
    }
}
