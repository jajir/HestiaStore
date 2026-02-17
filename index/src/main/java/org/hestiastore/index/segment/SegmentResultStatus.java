package org.hestiastore.index.segment;

/**
 * Outcome states for segment operations.
 */
public enum SegmentResultStatus {
    OK,
    BUSY,
    CLOSED,
    ERROR
}
