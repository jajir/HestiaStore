package org.hestiastore.index.segmentindex;

/**
 * Lifecycle states for the segment registry.
 */
enum SegmentRegistryState {
    /** Registry is operational and can serve requests. */
    READY,
    /** Registry is mutating the segment map and should be treated as busy. */
    FREEZE,
    /** Registry is permanently closed and will not accept work. */
    CLOSED,
    /** Registry has entered an error state and is no longer healthy. */
    ERROR
}
