package org.hestiastore.index.segmentindex.core.state;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * Index state representing a close sequence that rejects user operations while
 * still holding the directory lock until shutdown is fully finalized.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class IndexStateClosing<K, V> implements IndexState<K, V> {

    private final FileLock fileLock;

    IndexStateClosing(final FileLock fileLock) {
        this.fileLock = Vldtn.requireNonNull(fileLock, "fileLock");
    }

    /** {@inheritDoc} */
    @Override
    public SegmentIndexState state() {
        return SegmentIndexState.CLOSING;
    }

    /** {@inheritDoc} */
    @Override
    public IndexState<K, V> onReady() {
        throw new IllegalStateException(
                "Can't make ready index while it is closing.");
    }

    /** {@inheritDoc} */
    @Override
    public IndexState<K, V> onClose() {
        throw new IllegalStateException("Can't close already closing index.");
    }

    /** {@inheritDoc} */
    @Override
    public IndexState<K, V> finishClose() {
        if (fileLock.isLocked()) {
            fileLock.unlock();
        }
        return new IndexStateClosed<>();
    }

    /** {@inheritDoc} */
    @Override
    public void tryPerformOperation() {
        throw new IllegalStateException(
                "Can't perform operation while index is closing.");
    }

    FileLock getFileLock() {
        return fileLock;
    }
}
