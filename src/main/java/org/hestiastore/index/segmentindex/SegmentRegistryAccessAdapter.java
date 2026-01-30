package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentHandlerLockStatus;
import org.hestiastore.index.segmentregistry.SegmentRegistryImpl;

/**
 * Adapter that exposes split/maintenance registry operations without leaking
 * the concrete registry implementation into coordinators.
 */
final class SegmentRegistryAccessAdapter<K, V>
        implements SegmentRegistryAccess<K, V> {

    private final SegmentRegistryImpl<K, V> registry;

    SegmentRegistryAccessAdapter(final SegmentRegistryImpl<K, V> registry) {
        this.registry = Vldtn.requireNonNull(registry, "registry");
    }

    @Override
    public boolean isSegmentInstance(final SegmentId segmentId,
            final Segment<K, V> expected) {
        return registry.isSegmentInstance(segmentId, expected);
    }

    @Override
    public SegmentHandlerLockStatus lockSegmentHandler(
            final SegmentId segmentId, final Segment<K, V> expected) {
        return registry.lockSegmentHandler(segmentId, expected);
    }

    @Override
    public void unlockSegmentHandler(final SegmentId segmentId,
            final Segment<K, V> expected) {
        registry.unlockSegmentHandler(segmentId, expected);
    }
}
