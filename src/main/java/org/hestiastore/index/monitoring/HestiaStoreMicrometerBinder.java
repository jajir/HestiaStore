package org.hestiastore.index.monitoring;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.SegmentIndexState;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

/**
 * Micrometer binder exposing index operation counters from
 * {@link SegmentIndex#metricsSnapshot()}.
 */
public final class HestiaStoreMicrometerBinder implements MeterBinder {

    private final SegmentIndex<?, ?> index;
    private final String indexName;

    /**
     * Creates a binder for one index instance.
     *
     * @param index     source index
     * @param indexName logical index name used as metric tag
     */
    public HestiaStoreMicrometerBinder(final SegmentIndex<?, ?> index,
            final String indexName) {
        this.index = Vldtn.requireNonNull(index, "index");
        this.indexName = Vldtn.requireNonNull(indexName, "indexName");
    }

    /** {@inheritDoc} */
    @Override
    public void bindTo(final MeterRegistry registry) {
        Vldtn.requireNonNull(registry, "registry");

        FunctionCounter.builder("hestiastore_ops_get_total", index,
                i -> i.metricsSnapshot().getGetOperationCount())
                .description("Total number of get operations")
                .tag("index", indexName).register(registry);

        FunctionCounter.builder("hestiastore_ops_put_total", index,
                i -> i.metricsSnapshot().getPutOperationCount())
                .description("Total number of put operations")
                .tag("index", indexName).register(registry);

        FunctionCounter.builder("hestiastore_ops_delete_total", index,
                i -> i.metricsSnapshot().getDeleteOperationCount())
                .description("Total number of delete operations")
                .tag("index", indexName).register(registry);

        Gauge.builder("hestiastore_index_up", index,
                i -> isReady(i.getState()) ? 1D : 0D)
                .description("1 when index state is READY, else 0")
                .tag("index", indexName).register(registry);
    }

    private boolean isReady(final SegmentIndexState state) {
        return state == SegmentIndexState.READY;
    }
}
