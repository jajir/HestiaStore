package org.hestiastore.index.segmentindex;

/**
 * High-level lifecycle state of a SegmentIndex instance.
 */
public enum SegmentIndexState {
    /** Index is initializing or recovering state. */
    OPENING,
    /** Index is ready to serve operations. */
    READY,
    /** Index encountered an unrecoverable error. */
    ERROR,
    /** Index has been closed and rejects operations. */
    CLOSED
}
