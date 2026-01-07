package org.hestiastore.index.segment;

enum SegmentState {
    READY,
    FREEZE,
    MAINTENANCE_RUNNING,
    CLOSED,
    ERROR
}
