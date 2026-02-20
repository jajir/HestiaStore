package org.hestiastore.index.control.model;

import java.time.Instant;
import java.util.Map;

import org.hestiastore.index.Vldtn;

/**
 * Configuration snapshot for one index.
 */
public final class ConfigurationSnapshot {

    private final String indexName;
    private final Map<RuntimeSettingKey, Integer> values;
    private final long revision;
    private final Instant capturedAt;

    /**
     * Creates validated configuration snapshot.
     */
    public ConfigurationSnapshot(final String indexName,
            final Map<RuntimeSettingKey, Integer> values, final long revision,
            final Instant capturedAt) {
        this.indexName = Vldtn.requireNotBlank(indexName, "indexName");
        this.values = Map.copyOf(Vldtn.requireNonNull(values, "values"));
        this.revision = Vldtn.requireGreaterThanOrEqualToZero(revision,
                "revision");
        this.capturedAt = Vldtn.requireNonNull(capturedAt, "capturedAt");
    }

    public String getIndexName() {
        return indexName;
    }

    public Map<RuntimeSettingKey, Integer> getValues() {
        return values;
    }

    public long getRevision() {
        return revision;
    }

    public Instant getCapturedAt() {
        return capturedAt;
    }

    // Backward-compatible accessor style for existing call sites.
    public String indexName() {
        return indexName;
    }

    // Backward-compatible accessor style for existing call sites.
    public Map<RuntimeSettingKey, Integer> values() {
        return values;
    }

    // Backward-compatible accessor style for existing call sites.
    public long revision() {
        return revision;
    }

    // Backward-compatible accessor style for existing call sites.
    public Instant capturedAt() {
        return capturedAt;
    }
}
