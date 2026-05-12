package org.hestiastore.index.segmentindex.core.bootstrap;

/**
 * Persists the resolved configuration when create/open changed it.
 */
final class BootstrapStepWriteConfiguration<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    private final BootstrapConfigurationAccess<K, V> configurationAccess =
            new BootstrapConfigurationAccess<>();

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        if (state.isConfigurationWriteRequired()) {
            configurationAccess.save(request, state.getConfiguration());
        }
    }
}
