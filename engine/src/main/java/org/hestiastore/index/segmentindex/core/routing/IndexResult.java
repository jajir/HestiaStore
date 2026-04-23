package org.hestiastore.index.segmentindex.core.routing;

/**
 * Result wrapper for internal segment-index operations.
 *
 * @param <T> value type for successful results
 */
public final class IndexResult<T> {

    private final IndexResultStatus status;
    private final T value;

    private IndexResult(final IndexResultStatus status, final T value) {
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
    public static <T> IndexResult<T> ok(final T value) {
        return new IndexResult<>(IndexResultStatus.OK, value);
    }

    /**
     * Creates a successful result without a payload.
     *
     * @param <T> value type
     * @return successful result wrapper with {@code null} value
     */
    public static <T> IndexResult<T> ok() {
        return new IndexResult<>(IndexResultStatus.OK, null);
    }

    /**
     * Creates a result describing a temporarily busy runtime.
     *
     * @param <T> value type
     * @return busy result wrapper
     */
    public static <T> IndexResult<T> busy() {
        return new IndexResult<>(IndexResultStatus.BUSY, null);
    }

    /**
     * Creates a result describing a closed runtime.
     *
     * @param <T> value type
     * @return closed result wrapper
     */
    public static <T> IndexResult<T> closed() {
        return new IndexResult<>(IndexResultStatus.CLOSED, null);
    }

    /**
     * Creates a result describing an unrecoverable runtime error.
     *
     * @param <T> value type
     * @return error result wrapper
     */
    public static <T> IndexResult<T> error() {
        return new IndexResult<>(IndexResultStatus.ERROR, null);
    }

    /**
     * @return operation outcome status
     */
    public IndexResultStatus getStatus() {
        return status;
    }

    /**
     * @return payload carried by successful results or {@code null}
     */
    public T getValue() {
        return value;
    }
}
