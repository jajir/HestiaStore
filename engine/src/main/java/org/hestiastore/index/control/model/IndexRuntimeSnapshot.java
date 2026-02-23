package org.hestiastore.index.control.model;

import java.time.Instant;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * Immutable runtime snapshot for one index.
 */
public record IndexRuntimeSnapshot(String indexName, SegmentIndexState state,
        SegmentIndexMetricsSnapshot metrics, Instant capturedAt) {

    public IndexRuntimeSnapshot {
        indexName = Vldtn.requireNotBlank(indexName, "indexName");
        state = Vldtn.requireNonNull(state, "state");
        metrics = Vldtn.requireNonNull(metrics, "metrics");
        capturedAt = Vldtn.requireNonNull(capturedAt, "capturedAt");
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
}
