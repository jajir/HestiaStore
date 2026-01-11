package org.hestiastore.index.segmentindex;

import org.hestiastore.index.directory.FileLock;

/**
 * Index state that rejects all operations due to an unrecoverable failure.
 */
public class IndexStateError<K, V> implements IndexState<K, V> {

    private final Throwable failure;
    private final FileLock fileLock;

    IndexStateError(final Throwable failure, final FileLock fileLock) {
        this.failure = failure;
        this.fileLock = fileLock;
    }

    @Override
    public void onReady(final SegmentIndexImpl<K, V> index) {
        throw new IllegalStateException("Can't make ready index in ERROR.");
    }

    @Override
    public void onClose(final SegmentIndexImpl<K, V> index) {
        if (fileLock != null && fileLock.isLocked()) {
            fileLock.unlock();
        }
        index.setIndexState(new IndexStateClosed<>());
    }

    @Override
    public void tryPerformOperation() {
        throw new IllegalStateException("Index is in ERROR state.", failure);
    }
}
