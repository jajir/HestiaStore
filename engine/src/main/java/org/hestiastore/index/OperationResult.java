package org.hestiastore.index;

/**
 * Public operation result used by legacy segment and registry APIs.
 *
 * @param <T> value type returned by successful operations
 */
public final class OperationResult<T> {

    private final OperationStatus status;
    private final T value;

    private OperationResult(final OperationStatus status, final T value) {
        this.status = Vldtn.requireNonNull(status, "status");
        this.value = value;
    }

    /**
     * Creates an OK result with a value.
     *
     * @param value result value
     * @param <T> value type
     * @return result with OK status
     */
    public static <T> OperationResult<T> ok(final T value) {
        return new OperationResult<>(OperationStatus.OK, value);
    }

    /**
     * Creates an OK result without a value.
     *
     * @param <T> value type
     * @return result with OK status
     */
    public static <T> OperationResult<T> ok() {
        return new OperationResult<>(OperationStatus.OK, null);
    }

    /**
     * Creates a BUSY result.
     *
     * @param <T> value type
     * @return result with BUSY status
     */
    public static <T> OperationResult<T> busy() {
        return new OperationResult<>(OperationStatus.BUSY, null);
    }

    /**
     * Creates a CLOSED result.
     *
     * @param <T> value type
     * @return result with CLOSED status
     */
    public static <T> OperationResult<T> closed() {
        return new OperationResult<>(OperationStatus.CLOSED, null);
    }

    /**
     * Creates an ERROR result.
     *
     * @param <T> value type
     * @return result with ERROR status
     */
    public static <T> OperationResult<T> error() {
        return new OperationResult<>(OperationStatus.ERROR, null);
    }

    /**
     * Creates a result by status.
     *
     * @param status target status
     * @param <T> value type
     * @return result with the provided status
     */
    public static <T> OperationResult<T> fromStatus(
            final OperationStatus status) {
        final OperationStatus validated = Vldtn.requireNonNull(status,
                "status");
        if (validated == OperationStatus.OK) {
            return ok();
        }
        if (validated == OperationStatus.BUSY) {
            return busy();
        }
        if (validated == OperationStatus.CLOSED) {
            return closed();
        }
        return error();
    }

    /**
     * Returns the operation status.
     *
     * @return operation status
     */
    public OperationStatus getStatus() {
        return status;
    }

    /**
     * Returns the value for successful results, null otherwise.
     *
     * @return operation value or null
     */
    public T getValue() {
        return value;
    }

    /**
     * Returns true when the status is OK.
     *
     * @return true for successful results
     */
    public boolean isOk() {
        return status == OperationStatus.OK;
    }
}
