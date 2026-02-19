package org.hestiastore.monitoring.micrometer;

import java.util.Objects;

import org.hestiastore.index.monitoring.MonitoredIndex;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * {@link MonitoredIndex} backed by a live {@link SegmentIndex}.
 */
public final class MicrometerSegmentIndexSource implements MonitoredIndex {

    private final String indexName;
    private final SegmentIndex<?, ?> index;

    /**
     * Creates source for one live index.
     *
     * @param indexName logical index name
     * @param index     source index
     */
    public MicrometerSegmentIndexSource(final String indexName,
            final SegmentIndex<?, ?> index) {
        this.indexName = normalize(indexName, "indexName");
        this.index = Objects.requireNonNull(index, "index");
    }

    /** {@inheritDoc} */
    @Override
    public String indexName() {
        return indexName;
    }

    /** {@inheritDoc} */
    @Override
    public SegmentIndexState state() {
        return index.getState();
    }

    /** {@inheritDoc} */
    @Override
    public SegmentIndexMetricsSnapshot metricsSnapshot() {
        return index.metricsSnapshot();
    }

    private static String normalize(final String value, final String name) {
        final String normalized = Objects.requireNonNull(value, name).trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return normalized;
    }
}
