package org.hestiastore.index.segmentindex.core.bootstrap;

import java.util.Optional;

import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;

/**
 * Central factory for {@link SegmentIndex} construction and dependent
 * resources.
 * <p>
 * This class owns the public create/open entry points and starts the bootstrap
 * transaction that wires the managed close sequence used by returned index
 * instances.
 * </p>
 */
public final class SegmentIndexFactory {

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
    public static <M, N> SegmentIndex<M, N> create(final Directory directory,
            final IndexConfiguration<M, N> indexConf) {
        return bootstrapService(directory).create(indexConf);
    }

    /**
     * Creates a new index using an explicit chunk filter provider resolver.
     *
     * @param <M>                         key type
     * @param <N>                         value type
     * @param directory                   target index directory
     * @param indexConf                   user configuration overrides
     * @param chunkFilterProviderResolver resolver used to resolve persisted
     *                                    chunk filter specs
     * @return opened index instance
     */
    public static <M, N> SegmentIndex<M, N> create(final Directory directory,
            final IndexConfiguration<M, N> indexConf,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return bootstrapService(directory).create(indexConf,
                chunkFilterProviderResolver);
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
        return bootstrapService(directory).open(indexConf);
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
     *                                    chunk filter specs
     * @return opened index instance
     */
    public static <M, N> SegmentIndex<M, N> open(final Directory directory,
            final IndexConfiguration<M, N> indexConf,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return bootstrapService(directory).open(indexConf,
                chunkFilterProviderResolver);
    }

    /**
     * Opens an existing index using only persisted configuration.
     *
     * @param <M>       key type
     * @param <N>       value type
     * @param directory target index directory
     * @return opened index instance
     */
    public static <M, N> SegmentIndex<M, N> openStored(final Directory directory) {
        return bootstrapService(directory).openStored();
    }

    /**
     * Opens an existing index using only persisted configuration and an
     * explicit chunk filter provider resolver.
     *
     * @param <M>                         key type
     * @param <N>                         value type
     * @param directory                   target index directory
     * @param chunkFilterProviderResolver resolver used to resolve persisted
     *                                    chunk filter specs
     * @return opened index instance
     */
    public static <M, N> SegmentIndex<M, N> openStored(final Directory directory,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return bootstrapService(directory).openStored(chunkFilterProviderResolver);
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
        return bootstrapService(directory).tryOpen();
    }

    /**
     * Tries to open an index if configuration already exists in the directory.
     *
     * @param <M>                         key type
     * @param <N>                         value type
     * @param directory                   target index directory
     * @param chunkFilterProviderResolver resolver used to resolve persisted
     *                                    chunk filter specs
     * @return optional opened index
     */
    public static <M, N> Optional<SegmentIndex<M, N>> tryOpen(
            final Directory directory,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return bootstrapService(directory).tryOpen(chunkFilterProviderResolver);
    }

    private static SegmentIndexBootstrapService bootstrapService(
            final Directory directory) {
        return new SegmentIndexBootstrapService(directory);
    }
}
