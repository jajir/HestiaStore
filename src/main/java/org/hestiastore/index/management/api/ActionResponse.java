package org.hestiastore.index.management.api;

import java.time.Instant;

import org.hestiastore.index.Vldtn;

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
        final String id = Vldtn.requireNonNull(requestId, "requestId").trim();
        if (id.isEmpty()) {
            throw new IllegalArgumentException(
                    "requestId must not be blank");
        }
        requestId = id;
        action = Vldtn.requireNonNull(action, "action");
        status = Vldtn.requireNonNull(status, "status");
        message = message == null ? "" : message;
        capturedAt = Vldtn.requireNonNull(capturedAt, "capturedAt");
    }
}
