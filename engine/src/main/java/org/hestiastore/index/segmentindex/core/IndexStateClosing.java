package org.hestiastore.index.segmentindex.core;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.FileLock;

/**
 * Index state representing a close sequence that rejects user operations while
 * still holding the directory lock until shutdown is fully finalized.
 *
 * @param <K> key type
 * @param <V> value type
 */
record IndexStateClosing<K, V>(FileLock fileLock)
        implements IndexState<K, V> {

    IndexStateClosing {
        fileLock = Vldtn.requireNonNull(fileLock, "fileLock");
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

    FileLock getFileLock() {
        return fileLock;
    }
}
