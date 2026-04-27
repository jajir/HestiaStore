package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.slf4j.Logger;

/**
 * Explicit lifecycle resource shape for {@link SegmentIndexLifecycle}.
 * Closed and opened states are represented by different concrete types instead
 * of one holder with nullable fields.
 *
 * @param <K> key type
 * @param <V> value type
 */
abstract class SegmentIndexLifecycleResources<K, V> {

    @SuppressWarnings("unchecked")
    static <K, V> SegmentIndexLifecycleResources<K, V> empty() {
        return (SegmentIndexLifecycleResources<K, V>) ClosedSegmentIndexLifecycleResources
                .instance();
    }

    static <K, V> SegmentIndexLifecycleResources<K, V> opened(
            final Directory managedDirectory,
            final IndexConfiguration<K, V> indexConfiguration,
            final IndexRuntimeConfiguration<K, V> runtimeConfiguration,
            final ExecutorRegistry executorRegistry) {
        return new OpenedSegmentIndexLifecycleResources<>(managedDirectory,
                indexConfiguration, runtimeConfiguration, executorRegistry);
    }

    abstract Directory managedDirectory();

    abstract IndexConfiguration<K, V> indexConfiguration();

    abstract IndexRuntimeConfiguration<K, V> runtimeConfiguration();

    abstract ExecutorRegistry executorRegistry();

    abstract SegmentIndexLifecycleResources<K, V> requireOpened();

    abstract SegmentIndexLifecycleResources<K, V> close(Logger logger);
}
