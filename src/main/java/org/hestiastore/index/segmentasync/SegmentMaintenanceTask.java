package org.hestiastore.index.segmentasync;

/**
 * Maintenance operations scheduled for a segment.
 */
public enum SegmentMaintenanceTask {
    FLUSH,
    COMPACT,
    SPLIT
}
