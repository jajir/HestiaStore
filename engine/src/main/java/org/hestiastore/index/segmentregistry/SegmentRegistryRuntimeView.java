package org.hestiastore.index.segmentregistry;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentRuntimeLimits;

/**
 * Default runtime tuning view backed by the registry runtime.
 */
final class SegmentRegistryRuntimeView<K, V>
        implements SegmentRegistry.Runtime<K, V> {

    private final Consumer<SegmentRuntimeLimits> runtimeTuner;
    private final Supplier<List<BlockingSegment<K, V>>> loadedSegmentsSnapshot;
    private final Supplier<SegmentRegistryState> registryStateSupplier;

    /**
     * Creates a runtime view backed by registry collaborators.
     *
     * @param runtimeTuner runtime limit update consumer
     * @param loadedSegmentsSnapshot loaded segment snapshot supplier
     * @param registryStateSupplier current registry state supplier
     */
    SegmentRegistryRuntimeView(
            final Consumer<SegmentRuntimeLimits> runtimeTuner,
            final Supplier<List<BlockingSegment<K, V>>> loadedSegmentsSnapshot,
            final Supplier<SegmentRegistryState> registryStateSupplier) {
        this.runtimeTuner = Vldtn.requireNonNull(runtimeTuner, "runtimeTuner");
        this.loadedSegmentsSnapshot = Vldtn
                .requireNonNull(loadedSegmentsSnapshot,
                        "loadedSegmentsSnapshot");
        this.registryStateSupplier = Vldtn.requireNonNull(
                registryStateSupplier, "registryStateSupplier");
    }

    @Override
    public void updateRuntimeLimits(final SegmentRuntimeLimits runtimeLimits) {
        ensureRegistryReady();
        runtimeTuner.accept(runtimeLimits);
    }

    @Override
    public List<BlockingSegment<K, V>> loadedSegmentsSnapshot() {
        if (registryStateSupplier.get() != SegmentRegistryState.READY) {
            return List.of();
        }
        return loadedSegmentsSnapshot.get();
    }

    private void ensureRegistryReady() {
        final SegmentRegistryState state = registryStateSupplier.get();
        if (state != SegmentRegistryState.READY) {
            throw new IndexException(String.format(
                    "Segment registry is not ready for runtime updates: %s",
                    state));
        }
    }
}
