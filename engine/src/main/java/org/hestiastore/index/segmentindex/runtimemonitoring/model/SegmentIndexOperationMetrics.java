package org.hestiastore.index.segmentindex.runtimemonitoring.model;

/**
 * User-facing operation counters for one index.
 */
public final class SegmentIndexOperationMetrics {

    private final long readOperationCount;
    private final long putOperationCount;
    private final long deleteOperationCount;

    /**
     * Creates operation metrics.
     *
     * @param readOperationCount observed read operation count
     * @param putOperationCount observed put operation count
     * @param deleteOperationCount observed delete operation count
     */
    public SegmentIndexOperationMetrics(final long readOperationCount,
            final long putOperationCount, final long deleteOperationCount) {
        this.readOperationCount = MetricModelValidation.nonNegative(
                readOperationCount, "readOperationCount");
        this.putOperationCount = MetricModelValidation.nonNegative(
                putOperationCount, "putOperationCount");
        this.deleteOperationCount = MetricModelValidation.nonNegative(
                deleteOperationCount, "deleteOperationCount");
    }

    /**
     * Returns observed read operation count.
     *
     * @return read operation count
     */
    public long readOperationCount() {
        return readOperationCount;
    }

    /**
     * Returns observed put operation count.
     *
     * @return put operation count
     */
    public long putOperationCount() {
        return putOperationCount;
    }

    /**
     * Returns observed delete operation count.
     *
     * @return delete operation count
     */
    public long deleteOperationCount() {
        return deleteOperationCount;
    }
}
