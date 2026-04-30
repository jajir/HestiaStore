package org.hestiastore.index.segmentindex;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolverImpl;
import org.hestiastore.index.chunkstore.ChunkFilterSpec;

/**
 * Immutable chunk filter pipeline settings view.
 */
public final class IndexFilterConfiguration {

    private final List<ChunkFilterSpec> encodingChunkFilters;
    private final List<ChunkFilterSpec> decodingChunkFilters;
    private final ChunkFilterProviderResolver chunkFilterProviderResolver;

    /**
     * Creates filter configuration with the default chunk filter provider
     * resolver.
     *
     * @param encodingChunkFilters persisted encoding filter specs
     * @param decodingChunkFilters persisted decoding filter specs
     */
    public IndexFilterConfiguration(final List<ChunkFilterSpec> encodingChunkFilters,
            final List<ChunkFilterSpec> decodingChunkFilters) {
        this(encodingChunkFilters, decodingChunkFilters, null);
    }

    /**
     * Creates filter configuration with an optional runtime resolver.
     *
     * @param encodingChunkFilters persisted encoding filter specs
     * @param decodingChunkFilters persisted decoding filter specs
     * @param chunkFilterProviderResolver runtime resolver, or {@code null} to
     *                                    use the default resolver
     */
    public IndexFilterConfiguration(final List<ChunkFilterSpec> encodingChunkFilters,
            final List<ChunkFilterSpec> decodingChunkFilters,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        this.encodingChunkFilters = List.copyOf(encodingChunkFilters);
        this.decodingChunkFilters = List.copyOf(decodingChunkFilters);
        this.chunkFilterProviderResolver = chunkFilterProviderResolver;
    }

    /**
     * Returns persisted encoding filter specs.
     *
     * @return immutable encoding filter specs
     */
    public List<ChunkFilterSpec> encodingChunkFilterSpecs() {
        return encodingChunkFilters;
    }

    /**
     * Returns persisted decoding filter specs.
     *
     * @return immutable decoding filter specs
     */
    public List<ChunkFilterSpec> decodingChunkFilterSpecs() {
        return decodingChunkFilters;
    }

    /**
     * Returns configured runtime chunk filter provider resolver, or the default
     * resolver when none was specified.
     *
     * @return runtime chunk filter provider resolver
     */
    public ChunkFilterProviderResolver getChunkFilterProviderResolver() {
        return chunkFilterProviderResolver == null
                ? ChunkFilterProviderResolverImpl.defaultResolver()
                : chunkFilterProviderResolver;
    }
}
