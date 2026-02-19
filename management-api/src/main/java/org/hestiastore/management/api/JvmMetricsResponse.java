package org.hestiastore.management.api;

/**
 * JVM-wide metrics section shared by all indexes on one node.
 *
 * @param heapUsedBytes      current heap used bytes
 * @param heapCommittedBytes current heap committed bytes
 * @param heapMaxBytes       max heap bytes (Xmx)
 * @param nonHeapUsedBytes   current non-heap used bytes
 * @param gcCount            total GC collections
 * @param gcTimeMillis       total GC time in milliseconds
 */
public record JvmMetricsResponse(long heapUsedBytes, long heapCommittedBytes,
        long heapMaxBytes, long nonHeapUsedBytes, long gcCount,
        long gcTimeMillis) {

    /**
     * Creates validated JVM metrics payload.
     */
    public JvmMetricsResponse {
        requireNotNegative(heapUsedBytes, "heapUsedBytes");
        requireNotNegative(heapCommittedBytes, "heapCommittedBytes");
        requireNotNegative(heapMaxBytes, "heapMaxBytes");
        requireNotNegative(nonHeapUsedBytes, "nonHeapUsedBytes");
        requireNotNegative(gcCount, "gcCount");
        requireNotNegative(gcTimeMillis, "gcTimeMillis");
    }

    private static void requireNotNegative(final long value,
            final String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }
}
