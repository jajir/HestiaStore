package org.hestiastore.index.segmentindex.core.lifecycle;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.hestiastore.index.segmentindex.config.IndexConfigurationManager;
import org.hestiastore.index.segmentindex.config.IndexConfigurationStorage;

/**
 * Resolves persisted and runtime configuration for lifecycle-managed open
 * paths.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexLifecycleConfigurationResolver<K, V> {

    private final Directory directory;
    private final IndexConfiguration<K, V> userProvidedConfiguration;
    private final ChunkFilterProviderRegistry chunkFilterProviderRegistry;

    SegmentIndexLifecycleConfigurationResolver(final Directory directory,
            final IndexConfiguration<K, V> userProvidedConfiguration,
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.userProvidedConfiguration = Vldtn.requireNonNull(
                userProvidedConfiguration, "userProvidedConfiguration");
        this.chunkFilterProviderRegistry = Vldtn.requireNonNull(
                chunkFilterProviderRegistry, "chunkFilterProviderRegistry");
    }

    IndexConfiguration<K, V> loadConfiguration(final boolean createIndex) {
        return createIndex ? createConfigurationForNewIndex()
                : mergeWithStoredConfiguration();
    }

    IndexRuntimeConfiguration<K, V> resolveRuntimeConfiguration(
            final IndexConfiguration<K, V> configuration) {
        return Vldtn.requireNonNull(configuration, "configuration")
                .resolveRuntimeConfiguration(chunkFilterProviderRegistry);
    }

    private IndexConfiguration<K, V> createConfigurationForNewIndex() {
        final IndexConfigurationManager<K, V> configurationManager =
                configurationManager();
        final IndexConfiguration<K, V> configuration = configurationManager
                .applyDefaults(userProvidedConfiguration);
        configurationManager.save(configuration);
        return configuration;
    }

    private IndexConfiguration<K, V> mergeWithStoredConfiguration() {
        return configurationManager().mergeWithStored(userProvidedConfiguration);
    }

    private IndexConfigurationManager<K, V> configurationManager() {
        return new IndexConfigurationManager<>(
                new IndexConfigurationStorage<>(directory));
    }
}
