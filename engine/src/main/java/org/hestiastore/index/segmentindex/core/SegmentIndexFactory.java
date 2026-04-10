package org.hestiastore.index.segmentindex.core;

import java.util.Optional;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.config.DataTypeDescriptorRegistry;
import org.hestiastore.index.segmentindex.config.IndexConfigurationManager;
import org.hestiastore.index.segmentindex.config.IndexConfigurationStorage;

/**
 * Central lifecycle owner for SegmentIndex construction and dependent
 * resources.
 * <p>
 * This class owns the full startup/rollback flow for index create/open paths
 * and wires the managed close sequence used by returned index instances.
 * </p>
 */
public final class SegmentIndexFactory {

    private SegmentIndexFactory() {
    }

    /**
     * Creates a new index, persists resolved configuration, and opens runtime
     * services.
     *
     * @param <M>       key type
     * @param <N>       value type
     * @param directory target index directory
     * @param indexConf user configuration overrides
     * @return opened index instance
     */
    public static <M, N> SegmentIndex<M, N> create(final Directory directory,
            final IndexConfiguration<M, N> indexConf) {
        return create(directory, indexConf,
                ChunkFilterProviderRegistry.defaultRegistry());
    }

    /**
     * Creates a new index using an explicit chunk filter provider registry.
     *
     * @param <M> key type
     * @param <N> value type
     * @param directory target index directory
     * @param indexConf user configuration overrides
     * @param chunkFilterProviderRegistry registry used to resolve persisted
     *                                    chunk filter specs
     * @return opened index instance
     */
    public static <M, N> SegmentIndex<M, N> create(final Directory directory,
            final IndexConfiguration<M, N> indexConf,
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        final SegmentIndexLifecycle<M, N> lifecycle = new SegmentIndexLifecycle<>(
                directory, indexConf, chunkFilterProviderRegistry);
        lifecycle.open(true);
        return openIndex(lifecycle.getIndexConfiguration(),
                lifecycle.getIndexRuntimeConfiguration(), lifecycle);
    }

    /**
     * Opens an existing index by merging stored and user configuration.
     *
     * @param <M>       key type
     * @param <N>       value type
     * @param directory target index directory
     * @param indexConf user configuration overrides
     * @return opened index instance
     */
    public static <M, N> SegmentIndex<M, N> open(final Directory directory,
            final IndexConfiguration<M, N> indexConf) {
        return open(directory, indexConf,
                ChunkFilterProviderRegistry.defaultRegistry());
    }

    /**
     * Opens an existing index by merging stored and user configuration with an
     * explicit chunk filter provider registry.
     *
     * @param <M> key type
     * @param <N> value type
     * @param directory target index directory
     * @param indexConf user configuration overrides
     * @param chunkFilterProviderRegistry registry used to resolve persisted
     *                                    chunk filter specs
     * @return opened index instance
     */
    public static <M, N> SegmentIndex<M, N> open(final Directory directory,
            final IndexConfiguration<M, N> indexConf,
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        final SegmentIndexLifecycle<M, N> lifecycle = new SegmentIndexLifecycle<>(
                directory, indexConf, chunkFilterProviderRegistry);
        lifecycle.open(false);
        return openIndex(lifecycle.getIndexConfiguration(),
                lifecycle.getIndexRuntimeConfiguration(), lifecycle);
    }

    /**
     * Opens an existing index using configuration stored on disk.
     *
     * @param <M>       key type
     * @param <N>       value type
     * @param directory target index directory
     * @return opened index instance
     */
    public static <M, N> SegmentIndex<M, N> open(final Directory directory) {
        return open(directory, ChunkFilterProviderRegistry.defaultRegistry());
    }

    /**
     * Opens an existing index using only persisted configuration and an
     * explicit chunk filter provider registry.
     *
     * @param <M> key type
     * @param <N> value type
     * @param directory target index directory
     * @param chunkFilterProviderRegistry registry used to resolve persisted
     *                                    chunk filter specs
     * @return opened index instance
     */
    public static <M, N> SegmentIndex<M, N> open(final Directory directory,
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        final IndexConfigurationManager<M, N> confManager = new IndexConfigurationManager<>(
                new IndexConfigurationStorage<>(directory));
        final IndexConfiguration<M, N> conf = confManager.loadExisting();
        final SegmentIndexLifecycle<M, N> lifecycle = new SegmentIndexLifecycle<>(
                directory, conf, chunkFilterProviderRegistry);
        lifecycle.open(false);
        return openIndex(lifecycle.getIndexConfiguration(),
                lifecycle.getIndexRuntimeConfiguration(), lifecycle);
    }

    /**
     * Tries to open an index if configuration already exists in the directory.
     *
     * @param <M>       key type
     * @param <N>       value type
     * @param directory target index directory
     * @return optional opened index
     */
    public static <M, N> Optional<SegmentIndex<M, N>> tryOpen(
            final Directory directory) {
        return tryOpen(directory, ChunkFilterProviderRegistry.defaultRegistry());
    }

    /**
     * Tries to open an index if configuration already exists in the directory.
     *
     * @param <M> key type
     * @param <N> value type
     * @param directory target index directory
     * @param chunkFilterProviderRegistry registry used to resolve persisted
     *                                    chunk filter specs
     * @return optional opened index
     */
    public static <M, N> Optional<SegmentIndex<M, N>> tryOpen(
            final Directory directory,
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        final IndexConfigurationManager<M, N> confManager = new IndexConfigurationManager<>(
                new IndexConfigurationStorage<>(directory));
        final Optional<IndexConfiguration<M, N>> oConf = confManager
                .tryToLoad();
        if (oConf.isEmpty()) {
            return Optional.empty();
        }
        final SegmentIndexLifecycle<M, N> lifecycle = new SegmentIndexLifecycle<>(
                directory, oConf.get(), chunkFilterProviderRegistry);
        lifecycle.open(false);
        return Optional.of(openIndex(lifecycle.getIndexConfiguration(),
                lifecycle.getIndexRuntimeConfiguration(), lifecycle));
    }

    /**
     * Builds the runtime index stack from resolved configuration and lifecycle
     * resources.
     *
     * @param <M>       key type
     * @param <N>       value type
     * @param indexConf resolved configuration
     * @param runtimeConfiguration resolved runtime configuration
     * @param lifecycle lifecycle owning runtime dependencies
     * @return index wrapped with close adapters
     */
    private static <M, N> SegmentIndex<M, N> openIndex(
            final IndexConfiguration<M, N> indexConf,
            final IndexRuntimeConfiguration<M, N> runtimeConfiguration,
            final SegmentIndexLifecycle<M, N> lifecycle) {
        final Directory directoryFacade = lifecycle.getManagedDirectory();
        final TypeDescriptor<M> keyTypeDescriptor = DataTypeDescriptorRegistry
                .makeInstance(indexConf.getKeyTypeDescriptor());
        final TypeDescriptor<N> valueTypeDescriptor = DataTypeDescriptorRegistry
                .makeInstance(indexConf.getValueTypeDescriptor());
        Vldtn.requireNonNull(indexConf.isContextLoggingEnabled(),
                "isContextLoggingEnabled");
        SegmentIndex<M, N> index = new IndexInternalConcurrent<>(
                directoryFacade, keyTypeDescriptor, valueTypeDescriptor,
                indexConf, runtimeConfiguration,
                lifecycle.getManagedExecutorRegistry());
        if (Boolean.TRUE.equals(indexConf.isContextLoggingEnabled())) {
            Vldtn.requireNotBlank(indexConf.getIndexName(), "indexName");
            index = new IndexContextLoggingAdapter<>(indexConf, index);
        }
        final CloseableResource lifecycleOnClose = new AbstractCloseableResource() {
            @Override
            protected void doClose() {
                lifecycle.close();
            }
        };
        return new IndexDirectoryClosingAdapter<>(index, directoryFacade,
                lifecycleOnClose);
    }

}
