package org.hestiastore.index.segmentindex.core.bootstrap;

import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;

/**
 * Immutable inputs for one segment-index bootstrap run.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexBootstrapRequest<K, V> {

    private final Directory directory;
    private final IndexConfiguration<K, V> userProvidedConfiguration;
    private final ChunkFilterProviderResolver chunkFilterProviderResolver;
    private final SegmentIndexBootstrapMode mode;

    /**
     * Creates a bootstrap request.
     *
     * @param directory                   index directory
     * @param userProvidedConfiguration   user configuration request
     * @param chunkFilterProviderResolver optional custom chunk-filter resolver
     * @param mode                        bootstrap mode
     */
    SegmentIndexBootstrapRequest(final Directory directory,
            final IndexConfiguration<K, V> userProvidedConfiguration,
            final ChunkFilterProviderResolver chunkFilterProviderResolver,
            final SegmentIndexBootstrapMode mode) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        this.userProvidedConfiguration = Vldtn.requireNonNull(
                userProvidedConfiguration, "userProvidedConfiguration");
        this.chunkFilterProviderResolver = chunkFilterProviderResolver;
        this.mode = Vldtn.requireNonNull(mode, "mode");
    }

    Directory getDirectory() {
        return directory;
    }

    IndexConfiguration<K, V> getUserProvidedConfiguration() {
        return userProvidedConfiguration;
    }

    Optional<ChunkFilterProviderResolver> getChunkFilterProviderResolver() {
        return Optional.ofNullable(chunkFilterProviderResolver);
    }

    SegmentIndexBootstrapMode getMode() {
        return mode;
    }
}
