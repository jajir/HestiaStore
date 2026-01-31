package org.hestiastore.index.segmentregistry;

/**
 * Outcome states for segment registry operations.
 */
public enum SegmentRegistryResultStatus {
    /** Operation succeeded and any payload is valid. */
    OK,
    /** Registry is temporarily busy and the operation should be retried. */
    BUSY,
    /** Requested segment does not exist in the registry storage. */
    NOT_FOUND,
    /** Registry is closed and will not accept further operations. */
    CLOSED,
    /** Registry encountered an error while handling the request. */
    ERROR
}
