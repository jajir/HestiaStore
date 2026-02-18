package org.hestiastore.index.segmentindex;

/**
 * Result wrapper for internal segment-index operations.
 *
 * @param <T> value type for successful results
 */
final class IndexResult<T> {

    private final IndexResultStatus status;
    private final T value;

    private IndexResult(final IndexResultStatus status, final T value) {
        this.status = status;
        this.value = value;
    }

    static <T> IndexResult<T> ok(final T value) {
        return new IndexResult<>(IndexResultStatus.OK, value);
    }

    static <T> IndexResult<T> ok() {
        return new IndexResult<>(IndexResultStatus.OK, null);
    }

    static <T> IndexResult<T> busy() {
        return new IndexResult<>(IndexResultStatus.BUSY, null);
    }

    static <T> IndexResult<T> closed() {
        return new IndexResult<>(IndexResultStatus.CLOSED, null);
    }

    static <T> IndexResult<T> error() {
        return new IndexResult<>(IndexResultStatus.ERROR, null);
    }

    IndexResultStatus getStatus() {
        return status;
    }

    T getValue() {
        return value;
    }
}
