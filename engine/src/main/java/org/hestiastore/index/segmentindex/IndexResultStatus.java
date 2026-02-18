package org.hestiastore.index.segmentindex;

/**
 * Internal status for segment-index core operations.
 */
enum IndexResultStatus {
    OK,
    BUSY,
    CLOSED,
    ERROR
}
