package org.hestiastore.index.segmentindex;

import org.hestiastore.index.Vldtn;

/**
 * Builder section for logging settings.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexLoggingConfigurationBuilder<K, V> {

    private final IndexConfigurationBuilder<K, V> builder;

    IndexLoggingConfigurationBuilder(
            final IndexConfigurationBuilder<K, V> builder) {
        this.builder = Vldtn.requireNonNull(builder, "builder");
    }

    /**
     * Sets whether MDC-based context logging is enabled.
     *
     * @param value true when context logging is enabled
     * @return this section builder
     */
    public IndexLoggingConfigurationBuilder<K, V> contextEnabled(
            final Boolean value) {
        builder.setContextLoggingEnabled(value);
        return this;
    }
}
