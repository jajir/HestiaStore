package org.hestiastore.monitoring.json.api;

import java.util.Objects;

/**
 * Per-segment runtime metrics section inside an index report payload.
 */
public record SegmentRuntimeReportResponse(String segmentId, String state,
        long numberOfKeysInDeltaCache, long numberOfKeysInSegment,
        long numberOfKeysInScarceIndex, long numberOfKeysInSegmentCache,
        int numberOfKeysInWriteCache, int numberOfDeltaCacheFiles,
        long compactRequestCount, long flushRequestCount,
        long bloomFilterRequestCount, long bloomFilterRefusedCount,
        long bloomFilterPositiveCount, long bloomFilterFalsePositiveCount) {

    /**
     * Creates validated per-segment metrics payload.
     */
    public SegmentRuntimeReportResponse {
        segmentId = normalize(segmentId, "segmentId");
        state = normalize(state, "state");
        requireNotNegative(numberOfKeysInDeltaCache,
                "numberOfKeysInDeltaCache");
        requireNotNegative(numberOfKeysInSegment, "numberOfKeysInSegment");
        requireNotNegative(numberOfKeysInScarceIndex,
                "numberOfKeysInScarceIndex");
        requireNotNegative(numberOfKeysInSegmentCache,
                "numberOfKeysInSegmentCache");
        requireNotNegative(numberOfKeysInWriteCache, "numberOfKeysInWriteCache");
        requireNotNegative(numberOfDeltaCacheFiles, "numberOfDeltaCacheFiles");
        requireNotNegative(compactRequestCount, "compactRequestCount");
        requireNotNegative(flushRequestCount, "flushRequestCount");
        requireNotNegative(bloomFilterRequestCount, "bloomFilterRequestCount");
        requireNotNegative(bloomFilterRefusedCount, "bloomFilterRefusedCount");
        requireNotNegative(bloomFilterPositiveCount,
                "bloomFilterPositiveCount");
        requireNotNegative(bloomFilterFalsePositiveCount,
                "bloomFilterFalsePositiveCount");
    }

    private static String normalize(final String value, final String name) {
        final String normalized = Objects.requireNonNull(value, name).trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return normalized;
    }

    private static void requireNotNegative(final long value,
            final String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }
}
