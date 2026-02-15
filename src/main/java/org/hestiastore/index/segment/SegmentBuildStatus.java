package org.hestiastore.index.segment;

/**
 * Status for {@link SegmentBuilder} build attempts.
 */
public enum SegmentBuildStatus {
    /** Segment was built successfully. */
    OK,
    /** Segment directory is temporarily busy (for example lock held). */
    BUSY
}
