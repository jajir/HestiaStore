package org.hestiastore.index.segmentindex.core.stablesegment;

/**
 * Result wrapper for stable-segment operations.
 *
 * @param <T> value type for successful results
 */
public final class StableSegmentOperationResult<T> {

    private final StableSegmentOperationStatus status;
    private final T value;

    private StableSegmentOperationResult(
            final StableSegmentOperationStatus status,
            final T value) {
        this.status = status;
        this.value = value;
    }

    /**
     * Creates a successful result carrying a value.
     *
     * @param value returned value
     * @param <T> value type
     * @return successful result wrapper
     */
    public static <T> StableSegmentOperationResult<T> ok(final T value) {
        return new StableSegmentOperationResult<>(
                StableSegmentOperationStatus.OK, value);
    }

    /**
     * Creates a successful result without a payload.
     *
     * @param <T> value type
     * @return successful result wrapper with {@code null} value
     */
    public static <T> StableSegmentOperationResult<T> ok() {
        return new StableSegmentOperationResult<>(
                StableSegmentOperationStatus.OK, null);
    }

    /**
     * Creates a result describing a temporarily busy runtime.
     *
     * @param <T> value type
     * @return busy result wrapper
     */
    public static <T> StableSegmentOperationResult<T> busy() {
        return new StableSegmentOperationResult<>(
                StableSegmentOperationStatus.BUSY, null);
    }

    /**
     * Creates a result describing a closed runtime.
     *
     * @param <T> value type
     * @return closed result wrapper
     */
    public static <T> StableSegmentOperationResult<T> closed() {
        return new StableSegmentOperationResult<>(
                StableSegmentOperationStatus.CLOSED, null);
    }

    /**
     * Creates a result describing an unrecoverable runtime error.
     *
     * @param <T> value type
     * @return error result wrapper
     */
    public static <T> StableSegmentOperationResult<T> error() {
        return new StableSegmentOperationResult<>(
                StableSegmentOperationStatus.ERROR, null);
    }

    /**
     * @return operation outcome status
     */
    public StableSegmentOperationStatus getStatus() {
        return status;
    }

    /**
     * @return payload carried by successful results or {@code null}
     */
    public T getValue() {
        return value;
    }
}
