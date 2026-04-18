package org.hestiastore.index.segmentregistry;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentFullWriterTx;
import org.hestiastore.index.segment.SegmentId;

/**
 * Default materialization view backed by the registry runtime.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentRegistryMaterializationView<K, V>
        implements SegmentRegistry.Materialization<K, V> {

    private final SegmentIdAllocator segmentIdAllocator;
    private final PreparedSegmentWriterFactory<K, V> preparedSegmentWriterFactory;

    SegmentRegistryMaterializationView(
            final SegmentIdAllocator segmentIdAllocator,
            final PreparedSegmentWriterFactory<K, V> preparedSegmentWriterFactory) {
        this.segmentIdAllocator = Vldtn.requireNonNull(segmentIdAllocator,
                "segmentIdAllocator");
        this.preparedSegmentWriterFactory = Vldtn
                .requireNonNull(preparedSegmentWriterFactory,
                        "preparedSegmentWriterFactory");
    }

    @Override
    public SegmentId nextSegmentId() {
        return Vldtn.requireNonNull(segmentIdAllocator.nextId(), "segmentId");
    }

    @Override
    public SegmentFullWriterTx<K, V> openWriterTx(final SegmentId segmentId) {
        return preparedSegmentWriterFactory.openWriterTx(segmentId);
    }
}
