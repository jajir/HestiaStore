package org.hestiastore.index.segmentindex;

/**
 * Immutable logging settings view.
 */
public final class IndexLoggingConfiguration {

    private final Boolean contextEnabled;

    public IndexLoggingConfiguration(final Boolean contextEnabled) {
        this.contextEnabled = contextEnabled;
    }

    public Boolean contextEnabled() {
        return contextEnabled;
    }
}
