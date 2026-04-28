package org.hestiastore.index.segmentindex.core.session;

import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterProviderRegistry;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
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
     * Creates a new index using an explicit chunk filter provider registry.
     *
     * @param <M>                         key type
     * @param <N>                         value type
     * @param directory                   target index directory
     * @param indexConf                   user configuration overrides
     * @param chunkFilterProviderRegistry registry used to resolve persisted
     *                                    chunk filter specs
     * @return opened index instance
     */
    public static <M, N> SegmentIndex<M, N> create(final Directory directory,
            final IndexConfiguration<M, N> indexConf,
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        return SegmentIndexManagedIndexFactory.create(
                SegmentIndexLifecycleOpenFlow.startCreatedLifecycle(directory,
                        indexConf, chunkFilterProviderRegistry));
    }

    /**
     * Opens an existing index by merging stored and user configuration with an
     * explicit chunk filter provider registry.
     *
     * @param <M>                         key type
     * @param <N>                         value type
     * @param directory                   target index directory
     * @param indexConf                   user configuration overrides
     * @param chunkFilterProviderRegistry registry used to resolve persisted
     *                                    chunk filter specs
     * @return opened index instance
     */
    public static <M, N> SegmentIndex<M, N> open(final Directory directory,
            final IndexConfiguration<M, N> indexConf,
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        return SegmentIndexManagedIndexFactory.create(
                SegmentIndexLifecycleOpenFlow.startOpenedLifecycle(directory,
                        indexConf, chunkFilterProviderRegistry));
    }

    /**
     * Opens an existing index using only persisted configuration and an
     * explicit chunk filter provider registry.
     *
     * @param <M>                         key type
     * @param <N>                         value type
     * @param directory                   target index directory
     * @param chunkFilterProviderRegistry registry used to resolve persisted
     *                                    chunk filter specs
     * @return opened index instance
     */
    public static <M, N> SegmentIndex<M, N> openStored(final Directory directory,
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        return open(directory,
                loadExistingConfiguration(Vldtn.requireNonNull(directory,
                        "directory")),
                chunkFilterProviderRegistry);
    }

    /**
     * Tries to open an index if configuration already exists in the directory.
     *
     * @param <M>                         key type
     * @param <N>                         value type
     * @param directory                   target index directory
     * @param chunkFilterProviderRegistry registry used to resolve persisted
     *                                    chunk filter specs
     * @return optional opened index
     */
    public static <M, N> Optional<SegmentIndex<M, N>> tryOpen(
            final Directory directory,
            final ChunkFilterProviderRegistry chunkFilterProviderRegistry) {
        final Directory validatedDirectory = Vldtn.requireNonNull(directory,
                "directory");
        final ChunkFilterProviderRegistry validatedRegistry = Vldtn
                .requireNonNull(chunkFilterProviderRegistry,
                        "chunkFilterProviderRegistry");
        return SegmentIndexFactory.<M, N>tryLoadConfiguration(validatedDirectory)
                .map(configuration -> SegmentIndexFactory.<M, N>open(
                        validatedDirectory, configuration, validatedRegistry));
    }

    private static <M, N> IndexConfiguration<M, N> loadExistingConfiguration(
            final Directory directory) {
        return SegmentIndexFactory.<M, N>configurationManager(directory)
                .loadExisting();
    }

    private static <M, N> Optional<IndexConfiguration<M, N>> tryLoadConfiguration(
            final Directory directory) {
        return SegmentIndexFactory.<M, N>configurationManager(directory)
                .tryToLoad();
    }

    private static <M, N> IndexConfigurationManager<M, N> configurationManager(
            final Directory directory) {
        return new IndexConfigurationManager<>(
                new IndexConfigurationStorage<>(directory));
    }
}
