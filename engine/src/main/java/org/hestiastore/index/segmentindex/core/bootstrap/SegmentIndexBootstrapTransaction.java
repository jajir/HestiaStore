package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.ResolvedIndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.config.DataTypeDescriptorRegistry;
import org.hestiastore.index.segmentindex.config.IndexConfigurationManager;
import org.hestiastore.index.segmentindex.config.IndexConfigurationStorage;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.session.IndexContextLoggingAdapter;
import org.hestiastore.index.segmentindex.core.IndexMdcScopeRunner;
import org.hestiastore.index.segmentindex.core.session.IndexInternalConcurrent;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexResourceClosingAdapter;

/**
 * Opens one segment-index bootstrap transaction and owns cleanup until the
 * running index is returned to the caller.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexBootstrapTransaction<K, V> {

    private final Directory directory;
    private final IndexConfiguration<K, V> userProvidedConfiguration;
    private final ChunkFilterProviderResolver chunkFilterProviderResolver;
    private final ExecutorRegistryFactory<K, V> executorRegistryFactory;
    private final OpeningIndexFactory<K, V> openingIndexFactory;

    SegmentIndexBootstrapTransaction(final Directory directory,
            final IndexConfiguration<K, V> userProvidedConfiguration) {
        this(directory, userProvidedConfiguration, null);
    }

    SegmentIndexBootstrapTransaction(final Directory directory,
            final IndexConfiguration<K, V> userProvidedConfiguration,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.userProvidedConfiguration = Vldtn.requireNonNull(
                userProvidedConfiguration, "userProvidedConfiguration");
        this.chunkFilterProviderResolver = chunkFilterProviderResolver;
        this.executorRegistryFactory = this::createExecutorRegistry;
        this.openingIndexFactory = this::createOpeningIndex;
    }

    SegmentIndexBootstrapTransaction(final Directory directory,
            final IndexConfiguration<K, V> userProvidedConfiguration,
            final ChunkFilterProviderResolver chunkFilterProviderResolver,
            final ExecutorRegistryFactory<K, V> executorRegistryFactory,
            final OpeningIndexFactory<K, V> openingIndexFactory) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.userProvidedConfiguration = Vldtn.requireNonNull(
                userProvidedConfiguration, "userProvidedConfiguration");
        this.chunkFilterProviderResolver = chunkFilterProviderResolver;
        this.executorRegistryFactory = Vldtn.requireNonNull(
                executorRegistryFactory, "executorRegistryFactory");
        this.openingIndexFactory = Vldtn.requireNonNull(openingIndexFactory,
                "openingIndexFactory");
    }

    SegmentIndex<K, V> create() {
        return openSession(true);
    }

    SegmentIndex<K, V> open() {
        return openSession(false);
    }

    private SegmentIndex<K, V> openSession(final boolean createIndex) {
        final IndexConfiguration<K, V> configuration =
                loadConfiguration(createIndex);
        final ResolvedIndexConfiguration<K, V> runtimeConfiguration =
                resolveRuntimeConfiguration(configuration);
        final ExecutorRegistry executorRegistry =
                executorRegistryFactory.create(configuration);
        try {
            return new SegmentIndexResourceClosingAdapter<>(
                    createManagedIndex(configuration, runtimeConfiguration,
                            executorRegistry),
                    executorRegistry);
        } catch (final RuntimeException e) {
            closeExecutorRegistry(executorRegistry, e);
            throw e;
        }
    }

    private IndexConfiguration<K, V> loadConfiguration(
            final boolean createIndex) {
        return createIndex ? createConfigurationForNewIndex()
                : mergeWithStoredConfiguration();
    }

    private IndexConfiguration<K, V> createConfigurationForNewIndex() {
        final IndexConfigurationManager<K, V> manager = configurationManager();
        final IndexConfiguration<K, V> configuration = manager
                .applyDefaults(userProvidedConfiguration);
        manager.save(configuration);
        return configuration;
    }

    private IndexConfiguration<K, V> mergeWithStoredConfiguration() {
        return configurationManager().mergeWithStored(userProvidedConfiguration);
    }

    private IndexConfigurationManager<K, V> configurationManager() {
        return new IndexConfigurationManager<>(
                new IndexConfigurationStorage<>(directory));
    }

    private ResolvedIndexConfiguration<K, V> resolveRuntimeConfiguration(
            final IndexConfiguration<K, V> configuration) {
        return configuration.resolveRuntimeConfiguration(
                resolveProviderResolver(configuration));
    }

    private ChunkFilterProviderResolver resolveProviderResolver(
            final IndexConfiguration<K, V> configuration) {
        return chunkFilterProviderResolver == null
                ? configuration.filters().getChunkFilterProviderResolver()
                : chunkFilterProviderResolver;
    }

    private ExecutorRegistry createExecutorRegistry(
            final IndexConfiguration<K, V> configuration) {
        return ExecutorRegistry.builder()
                .withIndexName(configuration.identity().name())
                .withContextLoggingEnabled(Boolean.TRUE.equals(
                        configuration.logging().contextEnabled()))
                .withIndexMaintenanceThreads(
                        configuration.maintenance().indexThreads())
                .withSplitMaintenanceThreads(
                        configuration.maintenance().indexThreads())
                .withSegmentMaintenanceThreads(
                        configuration.maintenance().segmentThreads())
                .withRegistryMaintenanceThreads(
                        configuration.maintenance().registryLifecycleThreads())
                .build();
    }

    private IndexInternalConcurrent<K, V> createOpeningIndex(
            final Directory directoryFacade,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor,
            final IndexConfiguration<K, V> configuration,
            final ResolvedIndexConfiguration<K, V> runtimeConfiguration,
            final ExecutorRegistry executorRegistry) {
        return IndexInternalConcurrent.createOpening(
                directoryFacade,
                keyTypeDescriptor,
                valueTypeDescriptor,
                configuration,
                runtimeConfiguration,
                executorRegistry);
    }

    private SegmentIndex<K, V> createManagedIndex(
            final IndexConfiguration<K, V> configuration,
            final ResolvedIndexConfiguration<K, V> runtimeConfiguration,
            final ExecutorRegistry executorRegistry) {
        if (!Boolean.TRUE.equals(configuration.logging().contextEnabled())) {
            return createStartedIndex(configuration, runtimeConfiguration,
                    executorRegistry);
        }
        final IndexMdcScopeRunner contextScopeRunner =
                new IndexMdcScopeRunner(configuration.identity().name());
        return contextScopeRunner.supply(() -> new IndexContextLoggingAdapter<>(
                configuration,
                createStartedIndex(configuration, runtimeConfiguration,
                        executorRegistry)));
    }

    private IndexInternalConcurrent<K, V> createStartedIndex(
            final IndexConfiguration<K, V> configuration,
            final ResolvedIndexConfiguration<K, V> runtimeConfiguration,
            final ExecutorRegistry executorRegistry) {
        IndexInternalConcurrent<K, V> index = null;
        try {
            index = openingIndexFactory.create(
                    directory,
                    resolveKeyTypeDescriptor(configuration),
                    resolveValueTypeDescriptor(configuration),
                    configuration,
                    runtimeConfiguration,
                    executorRegistry);
            index.completeStartup();
            return index;
        } catch (final RuntimeException e) {
            cleanupFailedIndex(index, e);
            throw e;
        }
    }

    private TypeDescriptor<K> resolveKeyTypeDescriptor(
            final IndexConfiguration<K, V> configuration) {
        return DataTypeDescriptorRegistry
                .makeInstance(configuration.identity().keyTypeDescriptor());
    }

    private TypeDescriptor<V> resolveValueTypeDescriptor(
            final IndexConfiguration<K, V> configuration) {
        return DataTypeDescriptorRegistry
                .makeInstance(configuration.identity().valueTypeDescriptor());
    }

    private void cleanupFailedIndex(final IndexInternalConcurrent<K, V> index,
            final RuntimeException failure) {
        if (index == null) {
            return;
        }
        abortStartup(index, failure);
        closeIndex(index, failure);
    }

    private void abortStartup(final IndexInternalConcurrent<K, V> index,
            final RuntimeException failure) {
        try {
            index.abortStartup(failure);
        } catch (final RuntimeException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    private void closeIndex(final IndexInternalConcurrent<K, V> index,
            final RuntimeException failure) {
        if (index.wasClosed()) {
            return;
        }
        try {
            index.close();
        } catch (final RuntimeException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    private void closeExecutorRegistry(final ExecutorRegistry executorRegistry,
            final RuntimeException failure) {
        if (executorRegistry.wasClosed()) {
            return;
        }
        try {
            executorRegistry.close();
        } catch (final RuntimeException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }
}
