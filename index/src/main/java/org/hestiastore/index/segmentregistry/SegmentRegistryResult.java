package org.hestiastore.index.segmentregistry;

import org.hestiastore.index.Vldtn;

/**
 * Result wrapper for registry operations, carrying status and optional value.
 *
 * @param <T> value type for successful results
 */
public final class SegmentRegistryResult<T> {

    private final SegmentRegistryResultStatus status;
    private final T value;

    private SegmentRegistryResult(final SegmentRegistryResultStatus status,
            final T value) {
        this.status = Vldtn.requireNonNull(status, "status");
        this.value = value;
    }

    /**
     * Creates an OK result with value.
     *
     * @param value value
     * @param <T> value type
     * @return OK result with value
     */
    public static <T> SegmentRegistryResult<T> ok(final T value) {
        return new SegmentRegistryResult<>(SegmentRegistryResultStatus.OK,
                value);
    }

    /**
     * Creates an OK result without value.
     *
     * @param <T> value type
     * @return OK result
     */
    public static <T> SegmentRegistryResult<T> ok() {
        return new SegmentRegistryResult<>(SegmentRegistryResultStatus.OK, null);
    }

    /**
     * Creates a BUSY result.
     *
     * @param <T> value type
     * @return BUSY result
     */
    public static <T> SegmentRegistryResult<T> busy() {
        return new SegmentRegistryResult<>(SegmentRegistryResultStatus.BUSY,
                null);
    }

    /**
     * Creates a CLOSED result.
     *
     * @param <T> value type
     * @return CLOSED result
     */
    public static <T> SegmentRegistryResult<T> closed() {
        return new SegmentRegistryResult<>(SegmentRegistryResultStatus.CLOSED,
                null);
    }

    /**
     * Creates an ERROR result.
     *
     * @param <T> value type
     * @return ERROR result
     */
    public static <T> SegmentRegistryResult<T> error() {
        return new SegmentRegistryResult<>(SegmentRegistryResultStatus.ERROR,
                null);
    }

    /**
     * Creates a result by status.
     *
     * @param status result status
     * @param <T> value type
     * @return result with given status and null value
     */
    public static <T> SegmentRegistryResult<T> fromStatus(
            final SegmentRegistryResultStatus status) {
        final SegmentRegistryResultStatus validated = Vldtn.requireNonNull(
                status, "status");
        if (validated == SegmentRegistryResultStatus.OK) {
            return ok();
        }
        if (validated == SegmentRegistryResultStatus.BUSY) {
            return busy();
        }
        if (validated == SegmentRegistryResultStatus.CLOSED) {
            return closed();
        }
        return error();
    }

    /**
     * Returns status.
     *
     * @return status
     */
    public SegmentRegistryResultStatus getStatus() {
        return status;
    }

    /**
     * Returns value when status is OK, null otherwise.
     *
     * @return value or null
     */
    public T getValue() {
        return value;
    }

    /**
     * Returns true when status is OK.
     *
     * @return true for OK
     */
    public boolean isOk() {
        return status == SegmentRegistryResultStatus.OK;
    }
}
