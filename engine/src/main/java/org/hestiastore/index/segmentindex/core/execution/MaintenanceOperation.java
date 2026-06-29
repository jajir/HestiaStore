package org.hestiastore.index.segmentindex.core.execution;

/**
 * Segment maintenance operation handled by {@link MappedSegmentMaintenanceService}.
 */
enum MaintenanceOperation {
    COMPACT("compact", "Compact"),
    FLUSH("flush", "Flush");

    private final String id;
    private final String label;

    MaintenanceOperation(final String id, final String label) {
        this.id = id;
        this.label = label;
    }

    String id() {
        return id;
    }

    String label() {
        return label;
    }
}
