package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationResolution;

/**
 * Resolves the effective configuration without writing it.
 */
final class BootstrapStepResolveConfiguration<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private final BootstrapConfigurationAccess<K, V> configurationAccess =
            new BootstrapConfigurationAccess<>();

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        final IndexConfigurationResolution<K, V> resolution =
                configurationAccess.resolve(request);
        state.setConfiguration(resolution.configuration());
        state.setConfigurationWriteRequired(resolution.writeRequired());
    }
}
