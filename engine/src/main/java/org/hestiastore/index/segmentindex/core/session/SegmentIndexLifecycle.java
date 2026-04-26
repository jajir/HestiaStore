package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.hestiastore.index.segmentindex.core.executor.IndexExecutorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns initialization and close ordering for index startup dependencies.
 * <p>
 * The lifecycle opens configuration first, then executor registry, then
 * exposes the provided directory facade to the index runtime. Close happens in
 * reverse dependency order.
 * </p>
 *
 * @param <K> key type
 * @param <V> value type
 */
class SegmentIndexLifecycle<K, V> {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(SegmentIndexLifecycle.class);

    private final Directory directory;
    private final SegmentIndexLifecycleConfigurationResolver<K, V> configurationResolver;
    private SegmentIndexLifecycleResources<K, V> resources =
            SegmentIndexLifecycleResources.empty();

    /**
     * Creates a lifecycle backed by in-memory storage.
     *
     * @param indexConfiguration user-provided configuration overrides
     */
    SegmentIndexLifecycle(
            final IndexConfiguration<K, V> indexConfiguration) {
        this(new MemDirectory(), indexConfiguration,
                ChunkFilterProviderRegistry.defaultRegistry());
    }

    /**
     * Creates an in-memory lifecycle using an explicit chunk filter provider
     * registry.
     *
     * @param indexConfiguration user-provided configuration overrides
     * @param chunkFilterProviderRegistry registry used to resolve persisted
     *                                    chunk filter specs
     */
    SegmentIndexLifecycle(
            final IndexConfiguration<K, V> indexConfiguration,
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        this(new MemDirectory(), indexConfiguration, chunkFilterProviderRegistry);
    }

    /**
     * Creates a lifecycle over a specific backing directory.
     *
     * @param dir              backing directory
     * @param userProvidedConf user-provided configuration overrides
     */
    SegmentIndexLifecycle(final Directory dir,
            final IndexConfiguration<K, V> userProvidedConf) {
        this(dir, userProvidedConf,
                ChunkFilterProviderRegistry.defaultRegistry());
    }

    /**
     * Creates a lifecycle over a specific backing directory and explicit chunk
     * filter provider registry.
     *
     * @param dir backing directory
     * @param userProvidedConf user-provided configuration overrides
     * @param chunkFilterProviderRegistry registry used to resolve persisted
     *                                    chunk filter specs
     */
    SegmentIndexLifecycle(final Directory dir,
            final IndexConfiguration<K, V> userProvidedConf,
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        this.directory = Vldtn.requireNonNull(dir, "dir");
        this.configurationResolver =
                new SegmentIndexLifecycleConfigurationResolver<>(directory,
                        userProvidedConf, chunkFilterProviderRegistry);
    }

    /**
     * Creates a new index configuration, persists it, and opens managed
     * resources.
     */
    void createIndex() {
        openResources(true);
    }

    /**
     * Opens an existing index by merging stored and requested configuration,
     * then opens managed resources.
     */
    void openExistingIndex() {
        openResources(false);
    }

    private void openResources(final boolean createIndex) {
        try {
            final IndexConfiguration<K, V> indexConfiguration =
                    configurationResolver.loadConfiguration(createIndex);
            final IndexRuntimeConfiguration<K, V> runtimeConfiguration =
                    configurationResolver.resolveRuntimeConfiguration(
                            indexConfiguration);
            final IndexExecutorRegistry executorRegistry =
                    IndexExecutorRegistry.create(indexConfiguration);
            resources = SegmentIndexLifecycleResources.opened(directory,
                    indexConfiguration, runtimeConfiguration, executorRegistry);
        } catch (final RuntimeException e) {
            close();
            throw e;
        }
    }

    /**
     * Closes all managed resources.
     */
    void close() {
        resources = resources.close(LOGGER);
    }

    boolean isOpened() {
        return resources instanceof OpenedSegmentIndexLifecycleResources<?, ?>;
    }

    SegmentIndexLifecycleResources<K, V> openedResources() {
        return resources.requireOpened();
    }
}
