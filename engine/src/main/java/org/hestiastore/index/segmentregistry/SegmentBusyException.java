package org.hestiastore.index.segmentregistry;

/**
 * Signals that a segment operation cannot proceed because the target segment
 * or registry path is currently busy.
 * <p>
 * This exception is internal to the segment registry package and is translated
 * to {@link SegmentRegistryResultStatus#BUSY} at API boundaries.
 */
final class SegmentBusyException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a busy exception with the provided detail message.
     *
     * @param message detail message
     */
    SegmentBusyException(final String message) {
        super(message);
    }

}
