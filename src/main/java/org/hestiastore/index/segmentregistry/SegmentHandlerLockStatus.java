package org.hestiastore.index.segmentregistry;

/**
 * Outcome states for acquiring a segment handler lock.
 */
public enum SegmentHandlerLockStatus {
    /** Lock acquired successfully. */
    OK,
    /** Lock is already held by another caller. */
    BUSY
}
