package org.hestiastore.index.control.model;

import java.time.Instant;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * Immutable runtime snapshot for one index.
 */
public final class IndexRuntimeSnapshot {

    private final String indexName;
    private final SegmentIndexState state;
    private final SegmentIndexMetricsSnapshot metrics;
    private final Instant capturedAt;

    /**
     * Creates validated runtime snapshot.
     */
    public IndexRuntimeSnapshot(final String indexName,
            final SegmentIndexState state,
            final SegmentIndexMetricsSnapshot metrics,
            final Instant capturedAt) {
        this.indexName = Vldtn.requireNotBlank(indexName, "indexName");
        this.state = Vldtn.requireNonNull(state, "state");
        this.metrics = Vldtn.requireNonNull(metrics, "metrics");
        this.capturedAt = Vldtn.requireNonNull(capturedAt, "capturedAt");
    }

    public String getIndexName() {
        return indexName;
    }

    public SegmentIndexState getState() {
        return state;
    }

    public SegmentIndexMetricsSnapshot getMetrics() {
        return metrics;
    }

    public Instant getCapturedAt() {
        return capturedAt;
    }

    // Backward-compatible accessor style for existing call sites.
    public String indexName() {
        return indexName;
    }

    // Backward-compatible accessor style for existing call sites.
    public SegmentIndexState state() {
        return state;
    }

    // Backward-compatible accessor style for existing call sites.
    public SegmentIndexMetricsSnapshot metrics() {
        return metrics;
    }

    // Backward-compatible accessor style for existing call sites.
    public Instant capturedAt() {
        return capturedAt;
    }
}
