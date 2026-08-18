package org.hestiastore.index.segmentregistry;

import java.util.function.Supplier;

import org.hestiastore.index.IndexException;
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
    private final Supplier<SegmentRegistryState> registryStateSupplier;

    /**
     * Creates a materialization view backed by registry collaborators.
     *
     * @param segmentIdAllocator segment id allocator
     * @param preparedSegmentWriterFactory prepared writer factory
     * @param registryStateSupplier current registry state supplier
     */
    SegmentRegistryMaterializationView(
            final Supplier<SegmentId> segmentIdAllocator,
            final PreparedSegmentWriterFactory<K, V> preparedSegmentWriterFactory,
            final Supplier<SegmentRegistryState> registryStateSupplier) {
        this.segmentIdAllocator = Vldtn.requireNonNull(segmentIdAllocator,
                "segmentIdAllocator");
        this.preparedSegmentWriterFactory = Vldtn
                .requireNonNull(preparedSegmentWriterFactory,
                        "preparedSegmentWriterFactory");
        this.registryStateSupplier = Vldtn.requireNonNull(
                registryStateSupplier, "registryStateSupplier");
    }

    @Override
    public SegmentId nextSegmentId() {
        ensureRegistryReady();
        return Vldtn.requireNonNull(segmentIdAllocator.get(), "segmentId");
    }

    @Override
    public SegmentFullWriterTx<K, V> openWriterTx(final SegmentId segmentId) {
        ensureRegistryReady();
        return preparedSegmentWriterFactory.openWriterTx(segmentId);
    }

    private void ensureRegistryReady() {
        final SegmentRegistryState state = registryStateSupplier.get();
        if (state != SegmentRegistryState.READY) {
            throw new IndexException(String.format(
                    "Segment registry is not ready for materialization: %s",
                    state));
        }
    }
}
