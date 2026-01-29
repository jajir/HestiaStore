package org.hestiastore.index.segmentregistry;

/**
 * Outcome states for segment handler access.
 */
enum SegmentHandlerResultStatus {
    /** Operation succeeded and any payload is valid. */
    OK,
    /** Handler is locked and access is denied for non-privileged callers. */
    LOCKED
}
