package org.hestiastore.monitoring.json.api;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Runtime configuration view for one monitored index.
 */
public class ConfigViewResponse {

    private String indexName;
    private Map<String, Integer> original;
    private Map<String, Integer> current;
    private List<String> supportedKeys;
    private long revision;
    private Instant capturedAt;

    /**
     * No-arg constructor for JSON deserialization.
     */
    public ConfigViewResponse() {
        this.supportedKeys = List.of();
    }

    /**
     * Creates validated config view payload.
     */
    public ConfigViewResponse(final String indexName,
            final Map<String, Integer> original,
            final Map<String, Integer> current, final long revision,
            final Instant capturedAt) {
        this(indexName, original, current, List.of(), revision, capturedAt);
    }

    /**
     * Creates validated config view payload.
     */
    public ConfigViewResponse(final String indexName,
            final Map<String, Integer> original,
            final Map<String, Integer> current,
            final List<String> supportedKeys, final long revision,
            final Instant capturedAt) {
        setIndexName(indexName);
        setOriginal(original);
        setCurrent(current);
        setSupportedKeys(supportedKeys);
        setRevision(revision);
        setCapturedAt(capturedAt);
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(final String indexName) {
        this.indexName = normalize(indexName, "indexName");
    }

    public Map<String, Integer> getOriginal() {
        return original;
    }

    public void setOriginal(final Map<String, Integer> original) {
        this.original = Map.copyOf(Objects.requireNonNull(original, "original"));
    }

    public Map<String, Integer> getCurrent() {
        return current;
    }

    public void setCurrent(final Map<String, Integer> current) {
        this.current = Map.copyOf(Objects.requireNonNull(current, "current"));
    }

    public List<String> getSupportedKeys() {
        return supportedKeys;
    }

    public void setSupportedKeys(final List<String> supportedKeys) {
        this.supportedKeys = List
                .copyOf(Objects.requireNonNull(supportedKeys, "supportedKeys"));
    }

    public long getRevision() {
        return revision;
    }

    public void setRevision(final long revision) {
        if (revision < 0L) {
            throw new IllegalArgumentException("revision must be >= 0");
        }
        this.revision = revision;
    }

    public Instant getCapturedAt() {
        return capturedAt;
    }

    public void setCapturedAt(final Instant capturedAt) {
        this.capturedAt = Objects.requireNonNull(capturedAt, "capturedAt");
    }

    // Backward-compatible accessor style for existing call sites.
    public String indexName() {
        return indexName;
    }

    // Backward-compatible accessor style for existing call sites.
    public Map<String, Integer> original() {
        return original;
    }

    // Backward-compatible accessor style for existing call sites.
    public Map<String, Integer> current() {
        return current;
    }

    // Backward-compatible accessor style for existing call sites.
    public List<String> supportedKeys() {
        return supportedKeys;
    }

    // Backward-compatible accessor style for existing call sites.
    public long revision() {
        return revision;
    }

    // Backward-compatible accessor style for existing call sites.
    public Instant capturedAt() {
        return capturedAt;
    }

    private static String normalize(final String value, final String name) {
        final String normalized = Objects.requireNonNull(value, name).trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return normalized;
    }
}
