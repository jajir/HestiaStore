package org.hestiastore.management.api;

import java.time.Instant;

import java.util.Objects;

/**
 * Metrics payload returned by management API.
 *
 * @param indexName            logical index name
 * @param state                current index state
 * @param getOperationCount    number of get operations
 * @param putOperationCount    number of put operations
 * @param deleteOperationCount number of delete operations
 * @param capturedAt           snapshot timestamp
 */
public record MetricsResponse(String indexName, String state,
        long getOperationCount, long putOperationCount,
        long deleteOperationCount, Instant capturedAt) {

    /**
     * Creates validated metrics response payload.
     *
     * @param indexName            logical index name
     * @param state                current state name
     * @param getOperationCount    get count
     * @param putOperationCount    put count
     * @param deleteOperationCount delete count
     * @param capturedAt           capture time
     */
    public MetricsResponse {
        indexName = normalize(indexName, "indexName");
        state = normalize(state, "state");
        if (getOperationCount < 0) {
            throw new IllegalArgumentException(
                    "getOperationCount must be >= 0");
        }
        if (putOperationCount < 0) {
            throw new IllegalArgumentException(
                    "putOperationCount must be >= 0");
        }
        if (deleteOperationCount < 0) {
            throw new IllegalArgumentException(
                    "deleteOperationCount must be >= 0");
        }
        capturedAt = Objects.requireNonNull(capturedAt, "capturedAt");
    }

    private static String normalize(final String value, final String name) {
        final String normalized = Objects.requireNonNull(value, name).trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return normalized;
    }
}
