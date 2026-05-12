package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationManager;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationResolution;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationStorage;

/**
 * Shared access to persisted bootstrap configuration.
 */
final class BootstrapConfigurationAccess<K, V> {

    IndexConfigurationResolution<K, V> resolve(
            final SegmentIndexBootstrapRequest<K, V> request) {
        final SegmentIndexBootstrapRequest<K, V> nonNullRequest = Vldtn
                .requireNonNull(request, "request");
        if (nonNullRequest.getMode() == SegmentIndexBootstrapMode.CREATE) {
            return configurationManager(nonNullRequest).resolveForCreate(
                    nonNullRequest.getUserProvidedConfiguration());
        }
        return configurationManager(nonNullRequest).resolveForOpen(
                nonNullRequest.getUserProvidedConfiguration());
    }

    void save(final SegmentIndexBootstrapRequest<K, V> request,
            final EffectiveIndexConfiguration<K, V> configuration) {
        configurationManager(Vldtn.requireNonNull(request, "request")).save(
                Vldtn.requireNonNull(configuration, "configuration"));
    }

    private IndexConfigurationManager<K, V> configurationManager(
            final SegmentIndexBootstrapRequest<K, V> request) {
        return new IndexConfigurationManager<>(
                new IndexConfigurationStorage<>(request.getDirectory(),
                        resolveProviderResolver(request)));
    }

    private ChunkFilterProviderResolver resolveProviderResolver(
            final SegmentIndexBootstrapRequest<K, V> request) {
        return request.getChunkFilterProviderResolver().orElseGet(
                () -> request.getUserProvidedConfiguration().filters()
                        .getChunkFilterProviderResolver());
    }
}
