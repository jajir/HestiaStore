package org.hestiastore.index.segmentindex.core;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
import org.hestiastore.index.CloseableResource;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.IndexRuntimeConfiguration;
import org.hestiastore.index.segmentindex.config.IndexConfigurationManager;
import org.hestiastore.index.segmentindex.config.IndexConfigurationStorage;
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
public class SegmentIndexLifecycle<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Managed<Directory> managedDirectory;
    private Managed<IndexConfiguration<K, V>> managedConf;
    private Managed<IndexRuntimeConfiguration<K, V>> managedRuntimeConf;
    private Managed<IndexExecutorRegistry> managedExecutorRegistry;
    private Supplier<IndexConfiguration<K, V>> createIndexSupplier;

    /**
     * Creates a lifecycle backed by in-memory storage.
     *
     * @param indexConfiguration user-provided configuration overrides
     */
    public SegmentIndexLifecycle(
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
    public SegmentIndexLifecycle(
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
        managedConf//
                = new Managed<>(null, () -> {
                    IndexConfigurationManager<K, V> confManager = new IndexConfigurationManager<>(
                            new IndexConfigurationStorage<>(dir));
                    return confManager.mergeWithStored(userProvidedConf);
                }, toClose -> {
                    managedConf.resource = null;
                });

        createIndexSupplier = () -> {
            IndexConfigurationManager<K, V> confManager = new IndexConfigurationManager<>(
                    new IndexConfigurationStorage<>(dir));
            final IndexConfiguration<K, V> conf = confManager
                    .applyDefaults(userProvidedConf);
            confManager.save(conf);
            return conf;
        };

        managedRuntimeConf = new Managed<>(null, () -> managedConf.resource
                .resolveRuntimeConfiguration(chunkFilterProviderRegistry),
                toClose -> managedRuntimeConf.resource = null);

        managedExecutorRegistry//
                = new Managed<>(null, () -> {
                    return new IndexExecutorRegistry(managedConf.resource);
                }, toClose -> {
                    if (!toClose.wasClosed()) {
                        toClose.close();
                    }
                    managedExecutorRegistry.resource = null;
                });

        this.managedDirectory //
                = new Managed<>(null, () -> {
                    return dir;
                }, toClose -> {
                    if (toClose instanceof CloseableResource closeable
                            && !closeable.wasClosed()) {
                        closeable.close();
                    }
                    managedDirectory.resource = null;
                });
    }

    /**
     * Opens lifecycle-managed resources.
     *
     * @param createIndex when {@code true}, applies defaults and persists
     *                    configuration; otherwise merges with stored
     *                    configuration
     */
    public void open(final boolean createIndex) {
        try {
            if (createIndex) {
                managedConf.resource = createIndexSupplier.get();
            } else {
                managedConf.resource = managedConf.supplier.get();
            }
            managedRuntimeConf.resource = managedRuntimeConf.supplier.get();
            managedExecutorRegistry.resource = managedExecutorRegistry.supplier
                    .get();
            managedDirectory.resource = managedDirectory.supplier.get();
        } catch (final RuntimeException e) {
            close();
            throw e;
        }

    }

    /**
     * Closes all managed resources.
     */
    public void close() {
        managedDirectory.close();
        managedExecutorRegistry.close();
        managedRuntimeConf.close();
        managedConf.close();
    }

    /**
     * Returns the wrapped IO directory used by the running index.
     *
     * @return managed directory or {@code null} when not opened
     */
    public Directory getManagedDirectory() {
        return managedDirectory.resource;
    }

    /**
     * Returns the persisted index configuration loaded from or saved to index
     * metadata.
     *
     * @return persisted configuration or {@code null} when not opened
     */
    public IndexConfiguration<K, V> getIndexConfiguration() {
        return managedConf.resource;
    }

    /**
     * Returns the resolved runtime configuration.
     *
     * @return runtime configuration or {@code null} when not opened
     */
    public IndexRuntimeConfiguration<K, V> getIndexRuntimeConfiguration() {
        return managedRuntimeConf.resource;
    }

    /**
     * Returns executor registry used by the running index.
     *
     * @return executor registry or {@code null} when not opened
     */
    public IndexExecutorRegistry getManagedExecutorRegistry() {
        return managedExecutorRegistry.resource;
    }

    /**
     * Simple holder for resource value, creation supplier, and close action.
     *
     * @param <T> resource type
     */
    class Managed<T> {
        private T resource;
        private final Supplier<T> supplier;
        private final Consumer<T> onClose;

        /**
         * Creates managed wrapper.
         *
         * @param resource initial resource
         * @param supplier resource supplier
         * @param onClose  close callback
         */
        public Managed(final T resource, final Supplier<T> supplier,
                final Consumer<T> onClose) {
            this.resource = resource;
            this.supplier = supplier;
            this.onClose = onClose;
        }

        /**
         * Closes currently stored resource if present.
         */
        void close() {
            if (resource != null) {
                try {
                    onClose.accept(resource);
                } catch (final RuntimeException e) {
                    logger.error("Failed to close resource {}", resource, e);
                }
            }
        }

    }
}
