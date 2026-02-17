package org.hestiastore.index.management.api;

import java.time.Instant;

import org.hestiastore.index.Vldtn;

/**
 * Node lifecycle status payload.
 *
 * @param indexName   logical index name
 * @param state       current index state
 * @param ready       convenience flag used by UI/health checks
 * @param capturedAt  snapshot timestamp
 */
public record NodeStateResponse(String indexName, String state, boolean ready,
        Instant capturedAt) {

    /**
     * Creates validated node-state payload.
     *
     * @param indexName  logical index name
     * @param state      current state name
     * @param ready      true when node is ready for requests
     * @param capturedAt snapshot timestamp
     */
    public NodeStateResponse {
        indexName = normalize(indexName, "indexName");
        state = normalize(state, "state");
        capturedAt = Vldtn.requireNonNull(capturedAt, "capturedAt");
    }

    private static String normalize(final String value, final String name) {
        final String normalized = Vldtn.requireNonNull(value, name).trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return normalized;
    }
}
