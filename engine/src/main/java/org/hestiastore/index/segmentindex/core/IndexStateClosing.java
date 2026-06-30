package org.hestiastore.index.segmentindex.core;

import java.util.Objects;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileLock;

/**
 * Index state representing a close sequence that rejects user operations while
 * still holding the directory lock until shutdown is fully finalized.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class IndexStateClosing<K, V> implements IndexState<K, V> {

    private final FileLock fileLock;

    public IndexStateClosing(final FileLock fileLock) {
        this.fileLock = Vldtn.requireNonNull(fileLock, "fileLock");
    }

    public FileLock fileLock() {
        return fileLock;
    }

    FileLock getFileLock() {
        return fileLock();
    }

    /** {@inheritDoc} */
    @Override
    public void onReady(final SegmentIndexImpl<K, V> index) {
        throw new IllegalStateException(
                "Can't make ready index while it is closing.");
    }

    /** {@inheritDoc} */
    @Override
    public void onClose(final SegmentIndexImpl<K, V> index) {
        throw new IllegalStateException("Can't close already closing index.");
    }

    /** {@inheritDoc} */
    @Override
    public void tryPerformOperation() {
        throw new IllegalStateException(
                "Can't perform operation while index is closing.");
    }

    void finishClose(final SegmentIndexImpl<K, V> index) {
        if (fileLock.isLocked()) {
            fileLock.unlock();
        }
        index.setIndexState(new IndexStateClosed<>());
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof IndexStateClosing<?, ?> other)) {
            return false;
        }
        return Objects.equals(fileLock, other.fileLock);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileLock);
    }

    @Override
    public String toString() {
        return "IndexStateClosing[fileLock=" + fileLock + "]";
    }
}
