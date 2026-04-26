package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.hestiastore.index.segmentindex.core.executor.IndexExecutorRegistry;
import org.slf4j.Logger;

/**
 * Closed lifecycle shape with no opened resources.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class ClosedSegmentIndexLifecycleResources<K, V>
        extends SegmentIndexLifecycleResources<K, V> {

    private static final ClosedSegmentIndexLifecycleResources<?, ?> INSTANCE =
            new ClosedSegmentIndexLifecycleResources<>();

    private ClosedSegmentIndexLifecycleResources() {
    }

    static ClosedSegmentIndexLifecycleResources<?, ?> instance() {
        return INSTANCE;
    }

    @Override
    Directory managedDirectory() {
        return null;
    }

    @Override
    IndexConfiguration<K, V> indexConfiguration() {
        return null;
    }

    @Override
    IndexRuntimeConfiguration<K, V> runtimeConfiguration() {
        return null;
    }

    @Override
    IndexExecutorRegistry executorRegistry() {
        return null;
    }

    @Override
    SegmentIndexLifecycleResources<K, V> requireOpened() {
        throw new IllegalStateException(
                "SegmentIndexLifecycle resources are not opened.");
    }

    @Override
    SegmentIndexLifecycleResources<K, V> close(final Logger logger) {
        return this;
    }
}
