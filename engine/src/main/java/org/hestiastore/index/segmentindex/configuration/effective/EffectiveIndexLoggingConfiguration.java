package org.hestiastore.index.segmentindex.configuration.effective;

/**
 * Resolved logging settings.
 */
public final class EffectiveIndexLoggingConfiguration {

    private final boolean contextEnabled;

    public EffectiveIndexLoggingConfiguration(final boolean contextEnabled) {
        this.contextEnabled = contextEnabled;
    }

    public boolean contextEnabled() {
        return contextEnabled;
    }
}
