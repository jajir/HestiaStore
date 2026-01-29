package org.hestiastore.index.segmentregistry;

/**
 * State for segment handler access gate.
 */
public enum SegmentHandlerState {
    /** Segment access is available to non-privileged callers. */
    READY,
    /** Segment access is locked for non-privileged callers. */
    LOCKED
}
