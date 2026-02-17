package org.hestiastore.management.api;

import java.util.Objects;

/**
 * Request payload for action endpoints.
 *
 * @param requestId caller generated id used for tracing and idempotency
 */
public record ActionRequest(String requestId) {

    /**
     * Creates a validated action request.
     *
     * @param requestId caller generated id
     */
    public ActionRequest {
        final String value = Objects.requireNonNull(requestId, "requestId")
                .trim();
        if (value.isEmpty()) {
            throw new IllegalArgumentException(
                    "requestId must not be blank");
        }
        requestId = value;
    }
}
