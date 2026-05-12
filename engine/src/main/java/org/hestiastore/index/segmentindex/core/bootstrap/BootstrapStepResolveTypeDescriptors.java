package org.hestiastore.index.segmentindex.core.bootstrap;

import org.hestiastore.index.segmentindex.configuration.DataTypeDescriptorRegistry;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;

/**
 * Resolves persisted key and value type descriptor class names.
 */
final class BootstrapStepResolveTypeDescriptors<K, V>
        extends SegmentIndexBootstrapStep<K, V> {

    @Override
    void apply(final SegmentIndexBootstrapRequest<K, V> request,
            final SegmentIndexBootstrapState<K, V> state) {
        final EffectiveIndexConfiguration<K, V> configuration = state
                .getConfiguration();
        state.setKeyTypeDescriptor(DataTypeDescriptorRegistry
                .makeInstance(configuration.identity().keyTypeDescriptor()));
        state.setValueTypeDescriptor(DataTypeDescriptorRegistry
                .makeInstance(configuration.identity().valueTypeDescriptor()));
    }
}
