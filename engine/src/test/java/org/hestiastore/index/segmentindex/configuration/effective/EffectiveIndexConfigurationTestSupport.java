package org.hestiastore.index.segmentindex.configuration.effective;

import org.hestiastore.index.chunkstore.ChunkFilterProviderResolver;
import org.hestiastore.index.segmentindex.IndexConfiguration;

/**
 * Test helpers for crossing from user configuration requests to effective
 * runtime configuration.
 */
public final class EffectiveIndexConfigurationTestSupport {

    private EffectiveIndexConfigurationTestSupport() {
    }

    public static <K, V> EffectiveIndexConfiguration<K, V> effective(
            final IndexConfiguration<K, V> configuration) {
        return EffectiveIndexConfigurationResolver.resolveForCreate(
                configuration);
    }

    public static <K, V> EffectiveIndexConfiguration<K, V> effective(
            final IndexConfiguration<K, V> configuration,
            final ChunkFilterProviderResolver resolver) {
        return EffectiveIndexConfigurationResolver.resolveForCreate(
                configuration, resolver);
    }
}
