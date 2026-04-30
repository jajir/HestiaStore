package org.hestiastore.index.segmentindex.core.bootstrap;

import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.config.IndexConfigurationManager;
import org.hestiastore.index.segmentindex.config.IndexConfigurationStorage;

final class SegmentIndexBootstrapService {

    private final Directory directory;

    SegmentIndexBootstrapService(final Directory directory) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
    }

    <K, V> SegmentIndex<K, V> create(
            final IndexConfiguration<K, V> userProvidedConfiguration) {
        return transaction(userProvidedConfiguration, null).create();
    }

    <K, V> SegmentIndex<K, V> create(
            final IndexConfiguration<K, V> userProvidedConfiguration,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return transaction(userProvidedConfiguration,
                chunkFilterProviderResolver).create();
    }

    <K, V> SegmentIndex<K, V> open(
            final IndexConfiguration<K, V> userProvidedConfiguration) {
        return transaction(userProvidedConfiguration, null).open();
    }

    <K, V> SegmentIndex<K, V> open(
            final IndexConfiguration<K, V> userProvidedConfiguration,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return transaction(userProvidedConfiguration,
                chunkFilterProviderResolver).open();
    }

    <K, V> SegmentIndex<K, V> openStored() {
        return open(this.<K, V>loadExistingConfiguration());
    }

    <K, V> SegmentIndex<K, V> openStored(
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return open(this.<K, V>loadExistingConfiguration(),
                chunkFilterProviderResolver);
    }

    <K, V> Optional<SegmentIndex<K, V>> tryOpen() {
        return this.<K, V>tryLoadConfiguration()
                .map(configuration -> open(configuration));
    }

    <K, V> Optional<SegmentIndex<K, V>> tryOpen(
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        final ChunkFilterProviderResolver validatedResolver = Vldtn
                .requireNonNull(chunkFilterProviderResolver,
                        "chunkFilterProviderResolver");
        return this.<K, V>tryLoadConfiguration()
                .map(configuration -> open(configuration, validatedResolver));
    }

    private <K, V> IndexConfiguration<K, V> loadExistingConfiguration() {
        return this.<K, V>configurationManager().loadExisting();
    }

    private <K, V> Optional<IndexConfiguration<K, V>> tryLoadConfiguration() {
        return this.<K, V>configurationManager().tryToLoad();
    }

    private <K, V> IndexConfigurationManager<K, V> configurationManager() {
        return new IndexConfigurationManager<>(
                new IndexConfigurationStorage<>(directory));
    }

    private <K, V> SegmentIndexBootstrapTransaction<K, V> transaction(
            final IndexConfiguration<K, V> userProvidedConfiguration,
            final ChunkFilterProviderResolver chunkFilterProviderResolver) {
        return new SegmentIndexBootstrapTransaction<>(
                directory, Vldtn.requireNonNull(userProvidedConfiguration,
                        "userProvidedConfiguration"),
                chunkFilterProviderResolver);
    }
}
