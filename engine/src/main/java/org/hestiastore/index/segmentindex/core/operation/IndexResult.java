package org.hestiastore.index.segmentindex.core.operation;

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

    public static <T> IndexResult<T> ok(final T value) {
        return new IndexResult<>(IndexResultStatus.OK, value);
    }

    public static <T> IndexResult<T> ok() {
        return new IndexResult<>(IndexResultStatus.OK, null);
    }

    public static <T> IndexResult<T> busy() {
        return new IndexResult<>(IndexResultStatus.BUSY, null);
    }

    public static <T> IndexResult<T> closed() {
        return new IndexResult<>(IndexResultStatus.CLOSED, null);
    }

    public static <T> IndexResult<T> error() {
        return new IndexResult<>(IndexResultStatus.ERROR, null);
    }

    public IndexResultStatus getStatus() {
        return status;
    }

    public T getValue() {
        return value;
    }
}
