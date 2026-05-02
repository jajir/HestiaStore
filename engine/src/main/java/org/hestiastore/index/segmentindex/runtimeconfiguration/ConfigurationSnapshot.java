package org.hestiastore.index.segmentindex.runtimeconfiguration;

import java.time.Instant;
import java.util.Map;

import org.hestiastore.index.Vldtn;

/**
 * Configuration snapshot for one index.
 */
public record ConfigurationSnapshot(String indexName,
        Map<RuntimeSettingKey, Integer> values, long revision,
        Instant capturedAt) {

    public ConfigurationSnapshot {
        indexName = Vldtn.requireNotBlank(indexName, "indexName");
        values = Map.copyOf(Vldtn.requireNonNull(values, "values"));
        revision = Vldtn.requireGreaterThanOrEqualToZero(revision, "revision");
        capturedAt = Vldtn.requireNonNull(capturedAt, "capturedAt");
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
}
