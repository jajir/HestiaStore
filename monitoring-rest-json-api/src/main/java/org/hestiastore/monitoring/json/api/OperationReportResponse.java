package org.hestiastore.monitoring.json.api;

import java.beans.ConstructorProperties;

/**
 * Operation metrics section inside an index report payload.
 */
@SuppressWarnings("java:S6206")
public final class OperationReportResponse {

    private final long readOperationCount;
    private final long putOperationCount;
    private final long deleteOperationCount;

    /**
     * Creates operation metrics.
     *
     * @param readOperationCount read operation count
     * @param putOperationCount put operation count
     * @param deleteOperationCount delete operation count
     */
    @ConstructorProperties({ "readOperationCount", "putOperationCount",
            "deleteOperationCount" })
    public OperationReportResponse(final long readOperationCount,
            final long putOperationCount, final long deleteOperationCount) {
        this.readOperationCount = readOperationCount;
        this.putOperationCount = putOperationCount;
        this.deleteOperationCount = deleteOperationCount;
    }

    /**
     * Returns read operation count.
     *
     * @return read operation count
     */
    public long readOperationCount() {
        return readOperationCount;
    }

    /**
     * Returns put operation count.
     *
     * @return put operation count
     */
    public long putOperationCount() {
        return putOperationCount;
    }

    /**
     * Returns delete operation count.
     *
     * @return delete operation count
     */
    public long deleteOperationCount() {
        return deleteOperationCount;
    }
}
