package org.hestiastore.management.api;

import java.time.Instant;

import java.util.Objects;

/**
 * Error payload for management API responses.
 *
 * @param code       stable machine-readable error code
 * @param message    user-facing message
 * @param requestId  correlation id, can be empty when missing from request
 * @param capturedAt response timestamp
 */
public record ErrorResponse(String code, String message, String requestId,
        Instant capturedAt) {

    /**
     * Creates validated error payload.
     *
     * @param code       stable error code
     * @param message    response message
     * @param requestId  request correlation id
     * @param capturedAt response timestamp
     */
    public ErrorResponse {
        code = normalize(code, "code");
        message = normalize(message, "message");
        requestId = requestId == null ? "" : requestId.trim();
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
