package org.hestiastore.index.segmentindex.core.bootstrap;

import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;

/**
 * Internal bootstrap facade used by {@link SegmentIndexFactory} to create or
 * open segment indexes for one target directory.
 * <p>
 * The service keeps the public factory thin while centralizing validation,
 * persisted configuration loading, optional open semantics, and construction of
 * the bootstrap operation that wires runtime resources for the returned
 * index.
 * </p>
 */
final class SegmentIndexBootstrapService {

    private static final String CHUNK_FILTER_PROVIDER_RESOLVER =
            "chunkFilterProviderResolver";

    private final Directory directory;

    SegmentIndexBootstrapService(final Directory directory) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
    }

    <K, V> SegmentIndex<K, V> create(
            final IndexConfiguration<K, V> userProvidedConfiguration) {
        return operation(userProvidedConfiguration, null).create();
    }

    <K, V> SegmentIndex<K, V> create(
            final IndexConfiguration<K, V> userProvidedConfiguration,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        final ChunkFilterProviderResolver validatedResolver =
                requireChunkFilterProviderResolver(chunkFilterProviderResolver);
        return operation(userProvidedConfiguration, validatedResolver).create();
    }

    <K, V> SegmentIndex<K, V> open(
            final IndexConfiguration<K, V> userProvidedConfiguration) {
        return operation(userProvidedConfiguration, null).open();
    }

    <K, V> SegmentIndex<K, V> open(
            final IndexConfiguration<K, V> userProvidedConfiguration,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        final ChunkFilterProviderResolver validatedResolver =
                requireChunkFilterProviderResolver(chunkFilterProviderResolver);
        return operation(userProvidedConfiguration, validatedResolver).open();
    }

    <K, V> SegmentIndex<K, V> openStored() {
        return open(emptyConfiguration());
    }

    <K, V> SegmentIndex<K, V> openStored(
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        final ChunkFilterProviderResolver validatedResolver =
                requireChunkFilterProviderResolver(chunkFilterProviderResolver);
        return this.<K, V>operation(emptyConfiguration(), validatedResolver)
                .open();
    }

    <K, V> Optional<SegmentIndex<K, V>> tryOpen() {
        return this.<K, V>operation(emptyConfiguration(), null).tryOpen();
    }

    <K, V> Optional<SegmentIndex<K, V>> tryOpen(
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        final ChunkFilterProviderResolver validatedResolver =
                requireChunkFilterProviderResolver(chunkFilterProviderResolver);
        return this.<K, V>operation(emptyConfiguration(), validatedResolver)
                .tryOpen();
    }

    private <K, V> IndexConfiguration<K, V> emptyConfiguration() {
        return IndexConfiguration.<K, V>builder().build();
    }

    private ChunkFilterProviderResolver requireChunkFilterProviderResolver(
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return Vldtn.requireNonNull(chunkFilterProviderResolver,
                CHUNK_FILTER_PROVIDER_RESOLVER);
    }

    private <K, V> SegmentIndexBootstrapOperation<K, V> operation(
            final IndexConfiguration<K, V> userProvidedConfiguration,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return SegmentIndexBootstrapOperation.create(
                directory, Vldtn.requireNonNull(userProvidedConfiguration,
                        "userProvidedConfiguration"),
                chunkFilterProviderResolver);
    }
}
