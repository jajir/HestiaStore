package org.hestiastore.index.segmentregistry;

/**
 * Result wrapper for segment handler access.
 *
 * @param <T> value type for successful results
 */
final class SegmentHandlerResult<T> {

    private final SegmentHandlerResultStatus status;
    private final T value;

    private SegmentHandlerResult(final SegmentHandlerResultStatus status,
            final T value) {
        this.status = status;
        this.value = value;
    }

    /**
     * Creates a successful result with a value.
     *
     * @param value value for the successful result
     * @param <T>   value type
     * @return OK result with value
     */
    static <T> SegmentHandlerResult<T> ok(final T value) {
        return new SegmentHandlerResult<>(SegmentHandlerResultStatus.OK,
                value);
    }

    /**
     * Creates a LOCKED result.
     *
     * @param <T> value type
     * @return LOCKED result with null value
     */
    static <T> SegmentHandlerResult<T> locked() {
        return new SegmentHandlerResult<>(SegmentHandlerResultStatus.LOCKED,
                null);
    }

    /**
     * Returns the status of this result.
     *
     * @return status enum
     */
    SegmentHandlerResultStatus getStatus() {
        return status;
    }

    /**
     * Returns the value carried by this result, if any.
     *
     * @return value or null
     */
    T getValue() {
        return value;
    }

    /**
     * Returns true when this result is OK.
     *
     * @return true for OK results
     */
    boolean isOk() {
        return status == SegmentHandlerResultStatus.OK;
    }
}
