package org.hestiastore.management.api;

import java.time.Instant;

import java.util.Objects;

/**
 * Response payload for action requests.
 *
 * @param requestId  correlation id from request
 * @param action     action type
 * @param status     execution status
 * @param message    optional detail for UI/logs
 * @param capturedAt response creation time
 */
public record ActionResponse(String requestId, ActionType action,
        ActionStatus status, String message, Instant capturedAt) {

    /**
     * Creates a validated action response.
     *
     * @param requestId  request correlation id
     * @param action     action type
     * @param status     execution status
     * @param message    optional detail
     * @param capturedAt response timestamp
     */
    public ActionResponse {
        final String id = Objects.requireNonNull(requestId, "requestId").trim();
        if (id.isEmpty()) {
            throw new IllegalArgumentException(
                    "requestId must not be blank");
        }
        requestId = id;
        action = Objects.requireNonNull(action, "action");
        status = Objects.requireNonNull(status, "status");
        message = message == null ? "" : message;
        capturedAt = Objects.requireNonNull(capturedAt, "capturedAt");
    }
}
