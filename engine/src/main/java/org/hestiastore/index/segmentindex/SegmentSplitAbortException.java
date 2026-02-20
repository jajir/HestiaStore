package org.hestiastore.index.segmentindex;

/**
 * Signals that a split should be aborted without failing the caller.
 */
final class SegmentSplitAbortException extends RuntimeException {
	
    private static final long serialVersionUID = 1L;

	SegmentSplitAbortException(final String message) {
        super(message);
    }
}
