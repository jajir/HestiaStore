package org.hestiastore.index.segmentindex.configuration.persistence;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;

/**
 * Result of resolving an effective index configuration without persisting it.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class ResolvedIndexConfiguration<K, V> {

    private final EffectiveIndexConfiguration<K, V> configuration;
    private final boolean writeRequired;

    private ResolvedIndexConfiguration(
            final EffectiveIndexConfiguration<K, V> configuration,
            final boolean writeRequired) {
        this.configuration = Vldtn.requireNonNull(configuration,
                "configuration");
        this.writeRequired = writeRequired;
    }

    public static <K, V> ResolvedIndexConfiguration<K, V> of(
            final EffectiveIndexConfiguration<K, V> configuration,
            final boolean writeRequired) {
        return new ResolvedIndexConfiguration<>(configuration,
                writeRequired);
    }

    public EffectiveIndexConfiguration<K, V> configuration() {
        return configuration;
    }

    public boolean writeRequired() {
        return writeRequired;
    }
}
