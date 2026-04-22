package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;

/**
 * Owns only lifecycle startup flow so factory entrypoints do not need to know
 * how create/open transitions are executed.
 */
final class SegmentIndexLifecycleOpenFlow {

    private SegmentIndexLifecycleOpenFlow() {
    }

    static <M, N> SegmentIndexLifecycle<M, N> startCreatedLifecycle(
            final Directory directory,
            final IndexConfiguration<M, N> indexConfiguration,
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        final SegmentIndexLifecycle<M, N> lifecycle = new SegmentIndexLifecycle<>(
                Vldtn.requireNonNull(directory, "directory"),
                Vldtn.requireNonNull(indexConfiguration, "indexConfiguration"),
                Vldtn.requireNonNull(chunkFilterProviderRegistry,
                        "chunkFilterProviderRegistry"));
        lifecycle.createIndex();
        return lifecycle;
    }

    static <M, N> SegmentIndexLifecycle<M, N> startOpenedLifecycle(
            final Directory directory,
            final IndexConfiguration<M, N> indexConfiguration,
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        final SegmentIndexLifecycle<M, N> lifecycle = new SegmentIndexLifecycle<>(
                Vldtn.requireNonNull(directory, "directory"),
                Vldtn.requireNonNull(indexConfiguration, "indexConfiguration"),
                Vldtn.requireNonNull(chunkFilterProviderRegistry,
                        "chunkFilterProviderRegistry"));
        lifecycle.openExistingIndex();
        return lifecycle;
    }
}
