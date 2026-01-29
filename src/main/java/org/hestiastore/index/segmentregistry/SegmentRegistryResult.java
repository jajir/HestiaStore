package org.hestiastore.index.segmentregistry;

/**
 * Result wrapper for segment registry operations.
 *
 * @param <T> value type for successful results
 */
public final class SegmentRegistryResult<T> {

    private final SegmentRegistryResultStatus status;
    private final T value;

    private SegmentRegistryResult(final SegmentRegistryResultStatus status,
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
    public static <T> SegmentRegistryResult<T> ok(final T value) {
        return new SegmentRegistryResult<>(SegmentRegistryResultStatus.OK,
                value);
    }

    /**
     * Creates a successful result with no value.
     *
     * @param <T> value type
     * @return OK result with null value
     */
    public static <T> SegmentRegistryResult<T> ok() {
        return new SegmentRegistryResult<>(SegmentRegistryResultStatus.OK,
                null);
    }

    /**
     * Creates a BUSY result.
     *
     * @param <T> value type
     * @return BUSY result with null value
     */
    public static <T> SegmentRegistryResult<T> busy() {
        return new SegmentRegistryResult<>(SegmentRegistryResultStatus.BUSY,
                null);
    }

    /**
     * Creates a CLOSED result.
     *
     * @param <T> value type
     * @return CLOSED result with null value
     */
    public static <T> SegmentRegistryResult<T> closed() {
        return new SegmentRegistryResult<>(SegmentRegistryResultStatus.CLOSED,
                null);
    }

    /**
     * Creates an ERROR result.
     *
     * @param <T> value type
     * @return ERROR result with null value
     */
    public static <T> SegmentRegistryResult<T> error() {
        return new SegmentRegistryResult<>(SegmentRegistryResultStatus.ERROR,
                null);
    }

    /**
     * Returns the status of this result.
     *
     * @return status enum
     */
    public SegmentRegistryResultStatus getStatus() {
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
        return status == SegmentRegistryResultStatus.OK;
    }
}
