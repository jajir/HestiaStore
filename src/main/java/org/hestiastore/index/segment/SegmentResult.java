package org.hestiastore.index.segment;

/**
 * Result wrapper for segment operations, carrying status and optional value.
 *
 * @param <T> value type for successful results
 */
public final class SegmentResult<T> {

    private final SegmentResultStatus status;
    private final T value;

    /**
     * Creates a result with the given status and value.
     *
     * @param status result status
     * @param value optional value
     */
    private SegmentResult(final SegmentResultStatus status, final T value) {
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
    public static <T> SegmentResult<T> ok(final T value) {
        return new SegmentResult<>(SegmentResultStatus.OK, value);
    }

    /**
     * Creates a successful result with no value.
     *
     * @param <T> value type
     * @return OK result with null value
     */
    public static <T> SegmentResult<T> ok() {
        return new SegmentResult<>(SegmentResultStatus.OK, null);
    }

    /**
     * Creates a BUSY result.
     *
     * @param <T> value type
     * @return BUSY result with null value
     */
    public static <T> SegmentResult<T> busy() {
        return new SegmentResult<>(SegmentResultStatus.BUSY, null);
    }

    /**
     * Creates a CLOSED result.
     *
     * @param <T> value type
     * @return CLOSED result with null value
     */
    public static <T> SegmentResult<T> closed() {
        return new SegmentResult<>(SegmentResultStatus.CLOSED, null);
    }

    /**
     * Creates an ERROR result.
     *
     * @param <T> value type
     * @return ERROR result with null value
     */
    public static <T> SegmentResult<T> error() {
        return new SegmentResult<>(SegmentResultStatus.ERROR, null);
    }

    /**
     * Returns the status of this result.
     *
     * @return status enum
     */
    public SegmentResultStatus getStatus() {
        return status;
    }

    /**
     * Returns the value carried by this result, if any.
     *
     * @return value or null
     */
    public T getValue() {
        return value;
    }

    /**
     * Returns true when this result is OK.
     *
     * @return true for OK results
     */
    public boolean isOk() {
        return status == SegmentResultStatus.OK;
    }
}
