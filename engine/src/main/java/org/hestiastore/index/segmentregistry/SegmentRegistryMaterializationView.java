package org.hestiastore.index.segmentregistry;

import java.util.function.Supplier;

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

    private final Supplier<SegmentId> segmentIdAllocator;
    private final PreparedSegmentWriterFactory<K, V> preparedSegmentWriterFactory;

    SegmentRegistryMaterializationView(
            final Supplier<SegmentId> segmentIdAllocator,
            final PreparedSegmentWriterFactory<K, V> preparedSegmentWriterFactory) {
        this.segmentIdAllocator = Vldtn.requireNonNull(segmentIdAllocator,
                "segmentIdAllocator");
        this.preparedSegmentWriterFactory = Vldtn
                .requireNonNull(preparedSegmentWriterFactory,
                        "preparedSegmentWriterFactory");
    }

    @Override
    public SegmentId nextSegmentId() {
        return Vldtn.requireNonNull(segmentIdAllocator.get(), "segmentId");
    }

    @Override
    public SegmentFullWriterTx<K, V> openWriterTx(final SegmentId segmentId) {
        return preparedSegmentWriterFactory.openWriterTx(segmentId);
    }
}
