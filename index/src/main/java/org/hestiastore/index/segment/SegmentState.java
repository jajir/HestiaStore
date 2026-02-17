package org.hestiastore.index.segment;

/**
 * Internal lifecycle states for a segment instance.
 */
public enum SegmentState {
    /** Accepts reads and writes. */
    READY,
    /** Exclusive access for maintenance or full isolation iterators. */
    FREEZE,
    /** Background maintenance work is running. */
    MAINTENANCE_RUNNING,
    /** Segment is permanently closed. */
    CLOSED,
    /** Segment encountered a fatal error. */
    ERROR
}
