package org.hestiastore.management.api;

import java.util.Objects;

/**
 * Request payload for action endpoints.
 *
 * @param requestId caller generated id used for tracing and idempotency
 * @param indexName optional target index name; when blank/null all indexes are
 *                  targeted
 */
public record ActionRequest(String requestId, String indexName) {

    /**
     * Creates a validated action request.
     *
     * @param requestId caller generated id
     */
    public ActionRequest(final String requestId) {
        this(requestId, null);
    }

    /**
     * Creates a validated action request.
     *
     * @param requestId caller generated id
     * @param indexName optional target index name
     */
    public ActionRequest {
        final String id = Objects.requireNonNull(requestId, "requestId")
                .trim();
        if (id.isEmpty()) {
            throw new IllegalArgumentException(
                    "requestId must not be blank");
        }
        requestId = id;
        indexName = normalizeOptional(indexName);
    }

    private static String normalizeOptional(final String value) {
        if (value == null) {
            return null;
        }
        final String normalized = value.trim();
        return normalized.isEmpty() ? null : normalized;
    }
}
