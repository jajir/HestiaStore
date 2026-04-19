package org.hestiastore.index.segmentregistry;

import java.util.List;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentRuntimeLimits;

/**
 * Default runtime tuning view backed by the registry runtime.
 */
final class SegmentRegistryRuntimeView<K, V>
        implements SegmentRegistry.Runtime<K, V> {

    private final SegmentRuntimeTuner runtimeTuner;
    private final Supplier<List<SegmentHandle<K, V>>> loadedSegmentsSnapshot;

    SegmentRegistryRuntimeView(final SegmentRuntimeTuner runtimeTuner,
            final Supplier<List<SegmentHandle<K, V>>> loadedSegmentsSnapshot) {
        this.runtimeTuner = Vldtn.requireNonNull(runtimeTuner, "runtimeTuner");
        this.loadedSegmentsSnapshot = Vldtn
                .requireNonNull(loadedSegmentsSnapshot,
                        "loadedSegmentsSnapshot");
    }

    @Override
    public void updateRuntimeLimits(final SegmentRuntimeLimits runtimeLimits) {
        runtimeTuner.updateRuntimeLimits(runtimeLimits);
    }

    @Override
    public List<SegmentHandle<K, V>> loadedSegmentsSnapshot() {
        return loadedSegmentsSnapshot.get();
    }
}
