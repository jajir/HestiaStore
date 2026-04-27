package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.slf4j.Logger;

/**
 * Opened lifecycle shape with fully initialized runtime resources.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class OpenedSegmentIndexLifecycleResources<K, V>
        extends SegmentIndexLifecycleResources<K, V> {

    private final Directory managedDirectory;
    private final IndexConfiguration<K, V> indexConfiguration;
    private final IndexRuntimeConfiguration<K, V> runtimeConfiguration;
    private final ExecutorRegistry executorRegistry;

    OpenedSegmentIndexLifecycleResources(final Directory managedDirectory,
            final IndexConfiguration<K, V> indexConfiguration,
            final IndexRuntimeConfiguration<K, V> runtimeConfiguration,
            final ExecutorRegistry executorRegistry) {
        this.managedDirectory = Vldtn.requireNonNull(managedDirectory,
                "managedDirectory");
        this.indexConfiguration = Vldtn.requireNonNull(indexConfiguration,
                "indexConfiguration");
        this.runtimeConfiguration = Vldtn.requireNonNull(runtimeConfiguration,
                "runtimeConfiguration");
        this.executorRegistry = Vldtn.requireNonNull(executorRegistry,
                "executorRegistry");
    }

    @Override
    Directory managedDirectory() {
        return managedDirectory;
    }

    @Override
    IndexConfiguration<K, V> indexConfiguration() {
        return indexConfiguration;
    }

    @Override
    IndexRuntimeConfiguration<K, V> runtimeConfiguration() {
        return runtimeConfiguration;
    }

    @Override
    ExecutorRegistry executorRegistry() {
        return executorRegistry;
    }

    @Override
    SegmentIndexLifecycleResources<K, V> requireOpened() {
        return this;
    }

    @Override
    SegmentIndexLifecycleResources<K, V> close(final Logger logger) {
        final Logger lifecycleLogger = Vldtn.requireNonNull(logger, "logger");
        closeManagedDirectory(lifecycleLogger);
        closeExecutorRegistry(lifecycleLogger);
        return SegmentIndexLifecycleResources.empty();
    }

    private void closeExecutorRegistry(final Logger logger) {
        try {
            if (!executorRegistry.wasClosed()) {
                executorRegistry.close();
            }
        } catch (final RuntimeException e) {
            logger.error("Failed to close resource indexExecutorRegistry", e);
        }
    }

    private void closeManagedDirectory(final Logger logger) {
        try {
            if (managedDirectory instanceof CloseableResource closeable
                    && !closeable.wasClosed()) {
                closeable.close();
            }
        } catch (final RuntimeException e) {
            logger.error("Failed to close resource directory", e);
        }
    }
}
