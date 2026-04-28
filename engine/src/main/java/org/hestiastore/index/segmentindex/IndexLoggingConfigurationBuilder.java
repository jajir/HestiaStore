package org.hestiastore.index.segmentindex;

/**
 * Builder section for logging settings.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class IndexLoggingConfigurationBuilder<K, V> {

    private Boolean contextEnabled;

    IndexLoggingConfigurationBuilder() {
    }

    /**
     * Sets whether MDC-based context logging is enabled.
     *
     * @param value true when context logging is enabled
     * @return this section builder
     */
    public IndexLoggingConfigurationBuilder<K, V> contextEnabled(
            final Boolean value) {
        this.contextEnabled = value;
        return this;
    }

    IndexLoggingConfiguration build() {
        return new IndexLoggingConfiguration(contextEnabled);
    }
}
