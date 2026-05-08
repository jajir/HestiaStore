package org.hestiastore.index.segmentindex.core.bootstrap;

import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationManager;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationStorage;

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
        return operation(userProvidedConfiguration,
                chunkFilterProviderResolver).create();
    }

    <K, V> SegmentIndex<K, V> open(
            final IndexConfiguration<K, V> userProvidedConfiguration) {
        return operation(userProvidedConfiguration, null).open();
    }

    <K, V> SegmentIndex<K, V> open(
            final IndexConfiguration<K, V> userProvidedConfiguration,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return operation(userProvidedConfiguration,
                chunkFilterProviderResolver).open();
    }

    <K, V> SegmentIndex<K, V> openStored() {
        return open(emptyConfiguration());
    }

    <K, V> SegmentIndex<K, V> openStored(
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return open(emptyConfiguration(), chunkFilterProviderResolver);
    }

    <K, V> Optional<SegmentIndex<K, V>> tryOpen() {
        if (this.<K, V>configurationManager().tryToLoad().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(openStored());
    }

    <K, V> Optional<SegmentIndex<K, V>> tryOpen(
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        final ChunkFilterProviderResolver validatedResolver = Vldtn
                .requireNonNull(chunkFilterProviderResolver,
                        "chunkFilterProviderResolver");
        if (this.<K, V>configurationManager(validatedResolver)
                .tryToLoad().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(openStored(validatedResolver));
    }

    private <K, V> IndexConfigurationManager<K, V> configurationManager() {
        return new IndexConfigurationManager<>(
                new IndexConfigurationStorage<>(directory));
    }

    private <K, V> IndexConfigurationManager<K, V> configurationManager(
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return new IndexConfigurationManager<>(
                new IndexConfigurationStorage<>(directory,
                        chunkFilterProviderResolver));
    }

    private <K, V> IndexConfiguration<K, V> emptyConfiguration() {
        return IndexConfiguration.<K, V>builder().build();
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
