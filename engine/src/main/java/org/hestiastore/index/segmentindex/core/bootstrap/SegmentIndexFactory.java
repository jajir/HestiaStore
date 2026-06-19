package org.hestiastore.index.segmentindex.core.bootstrap;

import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;

/**
 * Central factory for index construction and dependent resources.
 * <p>
 * This class owns the public create/open entry points and starts the bootstrap
 * transaction that wires the managed close sequence used by returned index
 * instances.
 * </p>
 */
public final class SegmentIndexFactory {

    private static final String CHUNK_FILTER_PROVIDER_RESOLVER =
            "chunkFilterProviderResolver";

    private SegmentIndexFactory() {
    }

    /**
     * Creates a new index using the runtime resolver carried by the
     * configuration filter section.
     *
     * @param <M>       key type
     * @param <N>       value type
     * @param directory target index directory
     * @param indexConf user configuration overrides
     * @return opened index instance
     */
    public static <M, N> SegmentIndex<M, N> create(
            final Directory directory,
            final IndexConfiguration<M, N> indexConf) {
        return operation(directory, indexConf, null).create();
    }

    /**
     * Creates a new index using an explicit chunk filter provider resolver.
     *
     * @param <M>                         key type
     * @param <N>                         value type
     * @param directory                   target index directory
     * @param indexConf                   user configuration overrides
     * @param chunkFilterProviderResolver resolver used to resolve persisted
     *                                    chunk filter specs; must not be null
     * @return opened index instance
     */
    public static <M, N> SegmentIndex<M, N> create(
            final Directory directory,
            final IndexConfiguration<M, N> indexConf,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return operation(directory, indexConf,
                requireChunkFilterProviderResolver(
                        chunkFilterProviderResolver)).create();
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
    public static <M, N> SegmentIndex<M, N> open(
            final Directory directory,
            final IndexConfiguration<M, N> indexConf) {
        return operation(directory, indexConf, null).open();
    }

    /**
     * Opens an existing index by merging stored and user configuration with an
     * explicit chunk filter provider resolver.
     *
     * @param <M>                         key type
     * @param <N>                         value type
     * @param directory                   target index directory
     * @param indexConf                   user configuration overrides
     * @param chunkFilterProviderResolver resolver used to resolve persisted
     *                                    chunk filter specs; must not be null
     * @return opened index instance
     */
    public static <M, N> SegmentIndex<M, N> open(
            final Directory directory,
            final IndexConfiguration<M, N> indexConf,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return operation(directory, indexConf,
                requireChunkFilterProviderResolver(
                        chunkFilterProviderResolver)).open();
    }

    /**
     * Opens an existing index using only persisted configuration.
     *
     * @param <M>       key type
     * @param <N>       value type
     * @param directory target index directory
     * @return opened index instance
     */
    public static <M, N> SegmentIndex<M, N> openStored(
            final Directory directory) {
        return SegmentIndexFactory.<M, N>operation(directory,
                emptyConfiguration(), null).open();
    }

    /**
     * Opens an existing index using only persisted configuration and an
     * explicit chunk filter provider resolver.
     *
     * @param <M>                         key type
     * @param <N>                         value type
     * @param directory                   target index directory
     * @param chunkFilterProviderResolver resolver used to resolve persisted
     *                                    chunk filter specs; must not be null
     * @return opened index instance
     */
    public static <M, N> SegmentIndex<M, N> openStored(
            final Directory directory,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return SegmentIndexFactory.<M, N>operation(directory,
                emptyConfiguration(),
                requireChunkFilterProviderResolver(
                        chunkFilterProviderResolver)).open();
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
        return SegmentIndexFactory.<M, N>operation(directory,
                emptyConfiguration(), null).tryOpen();
    }

    /**
     * Tries to open an index if configuration already exists in the directory.
     *
     * @param <M>                         key type
     * @param <N>                         value type
     * @param directory                   target index directory
     * @param chunkFilterProviderResolver resolver used to resolve persisted
     *                                    chunk filter specs; must not be null
     * @return optional opened index
     */
    public static <M, N> Optional<SegmentIndex<M, N>> tryOpen(
            final Directory directory,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return SegmentIndexFactory.<M, N>operation(directory,
                emptyConfiguration(),
                requireChunkFilterProviderResolver(
                        chunkFilterProviderResolver)).tryOpen();
    }

    private static <M, N> SegmentIndexBootstrapOperation<M, N> operation(
            final Directory directory,
            final IndexConfiguration<M, N> userProvidedConfiguration,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return new SegmentIndexBootstrapOperation<>(
                Vldtn.requireNonNull(directory, "directory"),
                Vldtn.requireNonNull(userProvidedConfiguration,
                        "userProvidedConfiguration"),
                chunkFilterProviderResolver);
    }

    private static ChunkFilterProviderResolver requireChunkFilterProviderResolver(
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return Vldtn.requireNonNull(chunkFilterProviderResolver,
                CHUNK_FILTER_PROVIDER_RESOLVER);
    }

    private static <M, N> IndexConfiguration<M, N> emptyConfiguration() {
        return IndexConfiguration.<M, N>builder().build();
    }
}
