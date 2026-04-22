package org.hestiastore.index.segmentindex.core.session.state;

import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * Index state that rejects all operations due to an unrecoverable failure.
 */
class IndexStateError<K, V> implements IndexState<K, V> {

    private final Throwable failure;
    private final FileLock fileLock;

    IndexStateError(final Throwable failure, final FileLock fileLock) {
        this.failure = failure;
        this.fileLock = fileLock;
    }

    /** {@inheritDoc} */
    @Override
    public SegmentIndexState state() {
        return SegmentIndexState.ERROR;
    }

    /** {@inheritDoc} */
    @Override
    public IndexState<K, V> onReady() {
        throw new IllegalStateException("Can't make ready index in ERROR.");
    }

    /** {@inheritDoc} */
    @Override
    public IndexState<K, V> onClose() {
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public IndexState<K, V> finishClose() {
        if (fileLock != null && fileLock.isLocked()) {
            fileLock.unlock();
        }
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public void tryPerformOperation() {
        throw new IllegalStateException("Index is in ERROR state.", failure);
    }
}
