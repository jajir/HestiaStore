package org.hestiastore.index.management.api;

/**
 * Execution state for management action requests.
 */
public enum ActionStatus {
    ACCEPTED,
    COMPLETED,
    REJECTED,
    FAILED
}
