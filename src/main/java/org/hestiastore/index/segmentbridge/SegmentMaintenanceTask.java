package org.hestiastore.index.segmentbridge;

/**
 * Maintenance operations scheduled for a segment.
 */
public enum SegmentMaintenanceTask {
    FLUSH,
    COMPACT,
    SPLIT
}
