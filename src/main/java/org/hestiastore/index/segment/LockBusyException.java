package org.hestiastore.index.segment;

/**
 * Signals that a segment directory lock could not be acquired because it is
 * already held.
 */
final class LockBusyException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    LockBusyException(final String message) {
        super(message);
    }
}
