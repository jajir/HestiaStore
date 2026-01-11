package org.hestiastore.index.segmentindex;

/**
 * High-level lifecycle state of a SegmentIndex instance.
 */
public enum SegmentIndexState {
    OPENING,
    READY,
    ERROR,
    CLOSED
}
