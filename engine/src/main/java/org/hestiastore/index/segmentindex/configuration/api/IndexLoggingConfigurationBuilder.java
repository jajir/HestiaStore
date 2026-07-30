package org.hestiastore.index.segmentindex.configuration.api;

/**
 * Builder section for logging settings.
 */
public final class IndexLoggingConfigurationBuilder {

    private Boolean contextEnabled;

    IndexLoggingConfigurationBuilder() {
    }

    /**
     * Sets whether MDC-based context logging is enabled.
     *
     * @param value true when context logging is enabled
     * @return this section builder
     */
    public IndexLoggingConfigurationBuilder contextEnabled(
            final Boolean value) {
        this.contextEnabled = value;
        return this;
    }

    IndexLoggingConfiguration build() {
        return new IndexLoggingConfiguration(contextEnabled);
    }
}
